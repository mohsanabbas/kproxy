package kwire

// ConsumerProtocol is the application-level protocol embedded inside Kafka
// group-coordination frames. The blobs are NOT versioned by Kafka — they are
// versioned by the consumer client itself and exchanged opaquely as bytes.
//
// We only support versions 0..3 of ConsumerProtocolSubscription and 0..3 of
// ConsumerProtocolAssignment, which together cover every released Apache Kafka
// client (Java, librdkafka, Sarama, franz-go) since KIP-429 (cooperative
// sticky).

// Subscription is the decoded form of ConsumerProtocolSubscription.
//
//	v0: topics + userData
//	v1: + ownedPartitions      (introduced for KIP-429 cooperative sticky)
//	v2: + generationId
//	v3: + rackId
//
// Fields not present in the encoded version are left zero / nil.
type Subscription struct {
	Version           int16
	Topics            []string
	UserData          []byte // aliases the input buffer; copy if retained
	UserDataNull      bool
	OwnedPartitions   []TopicPartitions // v1+
	GenerationID      int32             // v2+
	GenerationPresent bool              // v2+: distinguishes 0 from absent
	RackID            string            // v3+
	RackIDNull        bool
	RackIDPresent     bool
}

// TopicPartitions is the (topic, []partition) pair used inside Subscription
// (ownedPartitions) and Assignment.
type TopicPartitions struct {
	Topic      string
	Partitions []int32
}

// DecodeSubscription decodes a ConsumerProtocolSubscription blob. It does not
// require the proxy to know which version the consumer client used — the
// version is the first 2 bytes of the blob.
//
// All fields that alias the input buffer (UserData) are documented as such.
func DecodeSubscription(blob []byte) (Subscription, error) {
	c := NewCursor(blob)
	s := Subscription{}
	v, err := c.ReadInt16()
	if err != nil {
		return s, err
	}
	s.Version = v

	// topics: ARRAY<STRING>
	n, _, err := c.ReadArrayLen()
	if err != nil {
		return s, err
	}
	if n > 0 {
		s.Topics = make([]string, n)
		for i := 0; i < n; i++ {
			t, err := c.ReadString()
			if err != nil {
				return s, err
			}
			s.Topics[i] = t
		}
	}

	// userData: NULLABLE_BYTES
	ud, isNull, err := c.ReadNullableBytes()
	if err != nil {
		return s, err
	}
	s.UserData = ud
	s.UserDataNull = isNull

	if v >= 1 && c.Remaining() > 0 {
		// ownedPartitions: ARRAY<{topic STRING, partitions ARRAY<INT32>}>
		op, err := decodeTopicPartitions(&c)
		if err != nil {
			return s, err
		}
		s.OwnedPartitions = op
	}
	if v >= 2 && c.Remaining() > 0 {
		gen, err := c.ReadInt32()
		if err != nil {
			return s, err
		}
		s.GenerationID = gen
		s.GenerationPresent = true
	}
	if v >= 3 && c.Remaining() > 0 {
		r, isNull, err := c.ReadNullableString()
		if err != nil {
			return s, err
		}
		s.RackID = r
		s.RackIDNull = isNull
		s.RackIDPresent = true
	}
	return s, nil
}

// AppendSubscription serialises a Subscription using its Version field.
func AppendSubscription(dst []byte, s Subscription) []byte {
	dst = AppendInt16(dst, s.Version)

	dst = AppendArrayLen(dst, len(s.Topics), false)
	for _, t := range s.Topics {
		dst = AppendString(dst, t)
	}
	dst = AppendNullableBytes(dst, s.UserData, s.UserDataNull)

	if s.Version >= 1 {
		dst = appendTopicPartitions(dst, s.OwnedPartitions)
	}
	if s.Version >= 2 {
		gen := s.GenerationID
		if !s.GenerationPresent {
			gen = -1
		}
		dst = AppendInt32(dst, gen)
	}
	if s.Version >= 3 {
		dst = AppendNullableString(dst, s.RackID, s.RackIDNull)
	}
	return dst
}

// Assignment is the decoded form of ConsumerProtocolAssignment.
//
//	v0..v2: partitions + userData
//	v3:     + rackId (the receiving member's rack, present at protocol level)
type Assignment struct {
	Version       int16
	Partitions    []TopicPartitions
	UserData      []byte
	UserDataNull  bool
	RackID        string // v3+
	RackIDNull    bool
	RackIDPresent bool
}

// DecodeAssignment decodes a ConsumerProtocolAssignment blob.
func DecodeAssignment(blob []byte) (Assignment, error) {
	c := NewCursor(blob)
	a := Assignment{}
	v, err := c.ReadInt16()
	if err != nil {
		return a, err
	}
	a.Version = v
	parts, err := decodeTopicPartitions(&c)
	if err != nil {
		return a, err
	}
	a.Partitions = parts
	ud, isNull, err := c.ReadNullableBytes()
	if err != nil {
		return a, err
	}
	a.UserData = ud
	a.UserDataNull = isNull
	if v >= 3 && c.Remaining() > 0 {
		r, isNull, err := c.ReadNullableString()
		if err != nil {
			return a, err
		}
		a.RackID = r
		a.RackIDNull = isNull
		a.RackIDPresent = true
	}
	return a, nil
}

// AppendAssignment serialises an Assignment using its Version field.
func AppendAssignment(dst []byte, a Assignment) []byte {
	dst = AppendInt16(dst, a.Version)
	dst = appendTopicPartitions(dst, a.Partitions)
	dst = AppendNullableBytes(dst, a.UserData, a.UserDataNull)
	if a.Version >= 3 {
		dst = AppendNullableString(dst, a.RackID, a.RackIDNull)
	}
	return dst
}

func decodeTopicPartitions(c *Cursor) ([]TopicPartitions, error) {
	n, _, err := c.ReadArrayLen()
	if err != nil {
		return nil, err
	}
	if n <= 0 {
		return nil, nil
	}
	out := make([]TopicPartitions, n)
	for i := 0; i < n; i++ {
		topic, err := c.ReadString()
		if err != nil {
			return nil, err
		}
		pn, _, err := c.ReadArrayLen()
		if err != nil {
			return nil, err
		}
		var parts []int32
		if pn > 0 {
			parts = make([]int32, pn)
			for j := 0; j < pn; j++ {
				p, err := c.ReadInt32()
				if err != nil {
					return nil, err
				}
				parts[j] = p
			}
		}
		out[i] = TopicPartitions{Topic: topic, Partitions: parts}
	}
	return out, nil
}

func appendTopicPartitions(dst []byte, tps []TopicPartitions) []byte {
	dst = AppendArrayLen(dst, len(tps), false)
	for _, tp := range tps {
		dst = AppendString(dst, tp.Topic)
		dst = AppendArrayLen(dst, len(tp.Partitions), false)
		for _, p := range tp.Partitions {
			dst = AppendInt32(dst, p)
		}
	}
	return dst
}
