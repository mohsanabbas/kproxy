package kwire

// MetadataResponse is the decoded form of MetadataResponse versions 0..12.
//
// Schema reference: Apache Kafka 3.9 MetadataResponse.json. Field availability
// follows the JSON exactly. Per-version notes:
//
//	v1+: Brokers.Rack, ControllerID, Topics.IsInternal
//	v2+: ClusterID
//	v3+: ThrottleTimeMs (prepended)
//	v5+: Partitions.OfflineReplicas
//	v7+: Partitions.LeaderEpoch
//	v8+: Topics.AuthorizedOperations, ClusterAuthorizedOperations (latter only v8-10)
//	v9+: flexible (compact strings, compact arrays, tagged fields)
//	v10+: Topics.TopicID (UUID, 16 bytes)
//	v12+: Topics.Name nullable
type MetadataResponse struct {
	Version                     int16
	ThrottleTimeMs              int32 // v3+
	Brokers                     []MetadataBroker
	ClusterID                   string // v2+
	ClusterIDNull               bool
	ControllerID                int32 // v1+; -1 if unset
	Topics                      []MetadataTopic
	ClusterAuthorizedOperations int32 // v8-10 only
}

type MetadataBroker struct {
	NodeID   int32
	Host     string
	Port     int32
	Rack     string // v1+
	RackNull bool
}

type MetadataTopic struct {
	ErrorCode            int16
	Name                 string // v12+ nullable; pre-v12 always present
	NameNull             bool   // only meaningful at v12+
	TopicID              [16]byte
	IsInternal           bool
	Partitions           []MetadataPartition
	AuthorizedOperations int32 // v8+
}

type MetadataPartition struct {
	ErrorCode       int16
	PartitionIndex  int32
	LeaderID        int32
	LeaderEpoch     int32 // v7+; -1 if unset
	ReplicaNodes    []int32
	IsrNodes        []int32
	OfflineReplicas []int32 // v5+
}

// DecodeMetadataResponse decodes a Metadata response body (after the response
// header has been stripped).
func DecodeMetadataResponse(body []byte, version int16) (MetadataResponse, error) {
	c := NewCursor(body)
	r := MetadataResponse{Version: version, ControllerID: -1}
	flex := IsFlexibleResponse(APIMetadata, version)

	if version >= 3 {
		v, err := c.ReadInt32()
		if err != nil {
			return r, err
		}
		r.ThrottleTimeMs = v
	}
	// brokers
	n, err := readArrLen(&c, flex)
	if err != nil {
		return r, err
	}
	if n > 0 {
		r.Brokers = make([]MetadataBroker, n)
		for i := range n {
			b, err := decodeMetadataBroker(&c, version, flex)
			if err != nil {
				return r, err
			}
			r.Brokers[i] = b
		}
	}
	if version >= 2 {
		var s string
		var isNull bool
		if flex {
			s, isNull, err = c.ReadCompactNullableString()
		} else {
			s, isNull, err = c.ReadNullableString()
		}
		if err != nil {
			return r, err
		}
		r.ClusterID = s
		r.ClusterIDNull = isNull
	}
	if version >= 1 {
		ctrl, err := c.ReadInt32()
		if err != nil {
			return r, err
		}
		r.ControllerID = ctrl
	}
	// topics
	n, err = readArrLen(&c, flex)
	if err != nil {
		return r, err
	}
	if n > 0 {
		r.Topics = make([]MetadataTopic, n)
		for i := range n {
			t, err := decodeMetadataTopic(&c, version, flex)
			if err != nil {
				return r, err
			}
			r.Topics[i] = t
		}
	}
	if version >= 8 && version <= 10 {
		v, err := c.ReadInt32()
		if err != nil {
			return r, err
		}
		r.ClusterAuthorizedOperations = v
	}
	if flex {
		if err := c.SkipTaggedFields(); err != nil {
			return r, err
		}
	}
	return r, nil
}

// AppendMetadataResponse encodes a Metadata response body using r.Version.
func AppendMetadataResponse(dst []byte, r MetadataResponse) []byte {
	flex := IsFlexibleResponse(APIMetadata, r.Version)
	if r.Version >= 3 {
		dst = AppendInt32(dst, r.ThrottleTimeMs)
	}
	dst = appendArrLen(dst, len(r.Brokers), flex)
	for i := range r.Brokers {
		dst = appendMetadataBroker(dst, r.Brokers[i], r.Version, flex)
	}
	if r.Version >= 2 {
		if flex {
			dst = AppendCompactNullableString(dst, r.ClusterID, r.ClusterIDNull)
		} else {
			dst = AppendNullableString(dst, r.ClusterID, r.ClusterIDNull)
		}
	}
	if r.Version >= 1 {
		dst = AppendInt32(dst, r.ControllerID)
	}
	dst = appendArrLen(dst, len(r.Topics), flex)
	for i := range r.Topics {
		dst = appendMetadataTopic(dst, r.Topics[i], r.Version, flex)
	}
	if r.Version >= 8 && r.Version <= 10 {
		dst = AppendInt32(dst, r.ClusterAuthorizedOperations)
	}
	if flex {
		dst = AppendEmptyTaggedFields(dst)
	}
	return dst
}

func decodeMetadataBroker(c *Cursor, version int16, flex bool) (MetadataBroker, error) {
	b := MetadataBroker{}
	id, err := c.ReadInt32()
	if err != nil {
		return b, err
	}
	b.NodeID = id
	if flex {
		b.Host, err = c.ReadCompactString()
	} else {
		b.Host, err = c.ReadString()
	}
	if err != nil {
		return b, err
	}
	if b.Port, err = c.ReadInt32(); err != nil {
		return b, err
	}
	if version >= 1 {
		var s string
		var isNull bool
		if flex {
			s, isNull, err = c.ReadCompactNullableString()
		} else {
			s, isNull, err = c.ReadNullableString()
		}
		if err != nil {
			return b, err
		}
		b.Rack = s
		b.RackNull = isNull
	}
	if flex {
		if err := c.SkipTaggedFields(); err != nil {
			return b, err
		}
	}
	return b, nil
}

func appendMetadataBroker(dst []byte, b MetadataBroker, version int16, flex bool) []byte {
	dst = AppendInt32(dst, b.NodeID)
	if flex {
		dst = AppendCompactString(dst, b.Host)
	} else {
		dst = AppendString(dst, b.Host)
	}
	dst = AppendInt32(dst, b.Port)
	if version >= 1 {
		if flex {
			dst = AppendCompactNullableString(dst, b.Rack, b.RackNull)
		} else {
			dst = AppendNullableString(dst, b.Rack, b.RackNull)
		}
	}
	if flex {
		dst = AppendEmptyTaggedFields(dst)
	}
	return dst
}

func decodeMetadataTopic(c *Cursor, version int16, flex bool) (MetadataTopic, error) {
	t := MetadataTopic{}
	ec, err := c.ReadInt16()
	if err != nil {
		return t, err
	}
	t.ErrorCode = ec
	// Name. v12+ nullable.
	if version >= 12 {
		var s string
		var isNull bool
		if flex {
			s, isNull, err = c.ReadCompactNullableString()
		} else {
			s, isNull, err = c.ReadNullableString()
		}
		if err != nil {
			return t, err
		}
		t.Name = s
		t.NameNull = isNull
	} else {
		if flex {
			t.Name, err = c.ReadCompactString()
		} else {
			t.Name, err = c.ReadString()
		}
		if err != nil {
			return t, err
		}
	}
	if version >= 10 {
		if c.Remaining() < 16 {
			return t, ErrTruncated
		}
		raw := c.Bytes()[c.Offset() : c.Offset()+16]
		copy(t.TopicID[:], raw)
		if err := c.Skip(16); err != nil {
			return t, err
		}
	}
	if version >= 1 {
		b, err := c.ReadInt8()
		if err != nil {
			return t, err
		}
		t.IsInternal = b != 0
	}
	// partitions
	n, err := readArrLen(c, flex)
	if err != nil {
		return t, err
	}
	if n > 0 {
		t.Partitions = make([]MetadataPartition, n)
		for i := range n {
			p, err := decodeMetadataPartition(c, version, flex)
			if err != nil {
				return t, err
			}
			t.Partitions[i] = p
		}
	}
	if version >= 8 {
		v, err := c.ReadInt32()
		if err != nil {
			return t, err
		}
		t.AuthorizedOperations = v
	}
	if flex {
		if err := c.SkipTaggedFields(); err != nil {
			return t, err
		}
	}
	return t, nil
}

func appendMetadataTopic(dst []byte, t MetadataTopic, version int16, flex bool) []byte {
	dst = AppendInt16(dst, t.ErrorCode)
	if version >= 12 {
		if flex {
			dst = AppendCompactNullableString(dst, t.Name, t.NameNull)
		} else {
			dst = AppendNullableString(dst, t.Name, t.NameNull)
		}
	} else {
		if flex {
			dst = AppendCompactString(dst, t.Name)
		} else {
			dst = AppendString(dst, t.Name)
		}
	}
	if version >= 10 {
		dst = append(dst, t.TopicID[:]...)
	}
	if version >= 1 {
		var b int8
		if t.IsInternal {
			b = 1
		}
		dst = AppendInt8(dst, b)
	}
	dst = appendArrLen(dst, len(t.Partitions), flex)
	for i := range t.Partitions {
		dst = appendMetadataPartition(dst, t.Partitions[i], version, flex)
	}
	if version >= 8 {
		dst = AppendInt32(dst, t.AuthorizedOperations)
	}
	if flex {
		dst = AppendEmptyTaggedFields(dst)
	}
	return dst
}

func decodeMetadataPartition(c *Cursor, version int16, flex bool) (MetadataPartition, error) {
	p := MetadataPartition{LeaderEpoch: -1}
	v16, err := c.ReadInt16()
	if err != nil {
		return p, err
	}
	p.ErrorCode = v16
	if p.PartitionIndex, err = c.ReadInt32(); err != nil {
		return p, err
	}
	if p.LeaderID, err = c.ReadInt32(); err != nil {
		return p, err
	}
	if version >= 7 {
		if p.LeaderEpoch, err = c.ReadInt32(); err != nil {
			return p, err
		}
	}
	if p.ReplicaNodes, err = decodeInt32Array(c, flex); err != nil {
		return p, err
	}
	if p.IsrNodes, err = decodeInt32Array(c, flex); err != nil {
		return p, err
	}
	if version >= 5 {
		if p.OfflineReplicas, err = decodeInt32Array(c, flex); err != nil {
			return p, err
		}
	}
	if flex {
		if err := c.SkipTaggedFields(); err != nil {
			return p, err
		}
	}
	return p, nil
}

func appendMetadataPartition(dst []byte, p MetadataPartition, version int16, flex bool) []byte {
	dst = AppendInt16(dst, p.ErrorCode)
	dst = AppendInt32(dst, p.PartitionIndex)
	dst = AppendInt32(dst, p.LeaderID)
	if version >= 7 {
		dst = AppendInt32(dst, p.LeaderEpoch)
	}
	dst = appendInt32Array(dst, p.ReplicaNodes, flex)
	dst = appendInt32Array(dst, p.IsrNodes, flex)
	if version >= 5 {
		dst = appendInt32Array(dst, p.OfflineReplicas, flex)
	}
	if flex {
		dst = AppendEmptyTaggedFields(dst)
	}
	return dst
}

// readArrLen / appendArrLen are tiny helpers that pick the right array
// encoding based on whether the surrounding RPC version is flexible. Extracted
// because Metadata in particular has 4 array sites and the boilerplate hurts
// readability.
func readArrLen(c *Cursor, flex bool) (int, error) {
	if flex {
		n, _, err := c.ReadCompactArrayLen()
		return n, err
	}
	n, _, err := c.ReadArrayLen()
	return n, err
}

func appendArrLen(dst []byte, n int, flex bool) []byte {
	if flex {
		return AppendCompactArrayLen(dst, n, false)
	}
	return AppendArrayLen(dst, n, false)
}

func decodeInt32Array(c *Cursor, flex bool) ([]int32, error) {
	n, err := readArrLen(c, flex)
	if err != nil {
		return nil, err
	}
	if n <= 0 {
		return nil, nil
	}
	out := make([]int32, n)
	for i := range n {
		v, err := c.ReadInt32()
		if err != nil {
			return nil, err
		}
		out[i] = v
	}
	return out, nil
}

func appendInt32Array(dst []byte, xs []int32, flex bool) []byte {
	dst = appendArrLen(dst, len(xs), flex)
	for _, v := range xs {
		dst = AppendInt32(dst, v)
	}
	return dst
}
