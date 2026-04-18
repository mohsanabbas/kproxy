package kwire

// ListOffsets v6 codec — kproxy is the client for telemetry collection, so we
// pin to v6 (first flexible version, schema stable through v9 for our purposes:
// we only ever ask for the latest offset, never max-timestamp/local-log/tiered).
//
// We only encode the request and only decode the response.

const ListOffsetsVersion int16 = 6

// OffsetLatest is the special timestamp value meaning "high water mark".
const OffsetLatest int64 = -1

// IsolationReadUncommitted is the only isolation level we use for telemetry —
// transactional records included.
const IsolationReadUncommitted int8 = 0

// ListOffsetsRequest (v6).
type ListOffsetsRequest struct {
	ReplicaID      int32 // -1 for normal consumer
	IsolationLevel int8
	Topics         []ListOffsetsTopic
}

type ListOffsetsTopic struct {
	Name       string
	Partitions []ListOffsetsPartition
}

type ListOffsetsPartition struct {
	PartitionIndex     int32
	CurrentLeaderEpoch int32 // -1 if unset
	Timestamp          int64 // OffsetLatest=-1 / OffsetEarliest=-2
}

// AppendListOffsetsRequest writes a ListOffsetsRequest body (v6). flex=true.
func AppendListOffsetsRequest(dst []byte, r ListOffsetsRequest) []byte {
	dst = AppendInt32(dst, r.ReplicaID)
	dst = AppendInt8(dst, r.IsolationLevel)
	dst = AppendCompactArrayLen(dst, len(r.Topics), false)
	for _, t := range r.Topics {
		dst = AppendCompactString(dst, t.Name)
		dst = AppendCompactArrayLen(dst, len(t.Partitions), false)
		for _, p := range t.Partitions {
			dst = AppendInt32(dst, p.PartitionIndex)
			dst = AppendInt32(dst, p.CurrentLeaderEpoch)
			dst = AppendInt64(dst, p.Timestamp)
			dst = AppendEmptyTaggedFields(dst)
		}
		dst = AppendEmptyTaggedFields(dst)
	}
	dst = AppendEmptyTaggedFields(dst)
	return dst
}

// ListOffsetsResponse (v6).
type ListOffsetsResponse struct {
	ThrottleTimeMs int32
	Topics         []ListOffsetsTopicResp
}

type ListOffsetsTopicResp struct {
	Name       string
	Partitions []ListOffsetsPartitionResp
}

type ListOffsetsPartitionResp struct {
	PartitionIndex int32
	ErrorCode      int16
	Timestamp      int64
	Offset         int64
	LeaderEpoch    int32
}

// DecodeListOffsetsResponse decodes a ListOffsets response body (v6).
func DecodeListOffsetsResponse(body []byte) (ListOffsetsResponse, error) {
	c := NewCursor(body)
	r := ListOffsetsResponse{}
	tt, err := c.ReadInt32()
	if err != nil {
		return r, err
	}
	r.ThrottleTimeMs = tt
	n, _, err := c.ReadCompactArrayLen()
	if err != nil {
		return r, err
	}
	if n > 0 {
		r.Topics = make([]ListOffsetsTopicResp, n)
		for i := range n {
			t := ListOffsetsTopicResp{}
			t.Name, err = c.ReadCompactString()
			if err != nil {
				return r, err
			}
			pn, _, err := c.ReadCompactArrayLen()
			if err != nil {
				return r, err
			}
			if pn > 0 {
				t.Partitions = make([]ListOffsetsPartitionResp, pn)
				for j := range pn {
					p := ListOffsetsPartitionResp{}
					if p.PartitionIndex, err = c.ReadInt32(); err != nil {
						return r, err
					}
					if p.ErrorCode, err = c.ReadInt16(); err != nil {
						return r, err
					}
					if p.Timestamp, err = c.ReadInt64(); err != nil {
						return r, err
					}
					if p.Offset, err = c.ReadInt64(); err != nil {
						return r, err
					}
					if p.LeaderEpoch, err = c.ReadInt32(); err != nil {
						return r, err
					}
					if err := c.SkipTaggedFields(); err != nil {
						return r, err
					}
					t.Partitions[j] = p
				}
			}
			if err := c.SkipTaggedFields(); err != nil {
				return r, err
			}
			r.Topics[i] = t
		}
	}
	if err := c.SkipTaggedFields(); err != nil {
		return r, err
	}
	return r, nil
}
