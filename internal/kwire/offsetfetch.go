package kwire

// OffsetFetch v8 codec - kproxy is the client. v8 is pinned to take advantage
// of the multi-group form (one RPC instead of N for telemetry).

const OffsetFetchVersion int16 = 8

// OffsetFetchRequest (v8). Topics may be nil within a group entry to mean "all
// topics for that group" (NULLABLE).
type OffsetFetchRequest struct {
	Groups        []OffsetFetchGroup
	RequireStable bool
}

type OffsetFetchGroup struct {
	GroupID    string
	Topics     []OffsetFetchTopicReq
	TopicsNull bool
}

type OffsetFetchTopicReq struct {
	Name             string
	PartitionIndexes []int32
}

func AppendOffsetFetchRequest(dst []byte, r OffsetFetchRequest) []byte {
	dst = AppendCompactArrayLen(dst, len(r.Groups), false)
	for _, g := range r.Groups {
		dst = AppendCompactString(dst, g.GroupID)
		// NULLABLE COMPACT_ARRAY: 0 means null, n+1 otherwise.
		if g.TopicsNull {
			dst = AppendUvarint(dst, 0)
		} else {
			dst = AppendUvarint(dst, uint32(len(g.Topics)+1)) // #nosec G115 -- bounded by frame.MaxFrameSize
			for _, t := range g.Topics {
				dst = AppendCompactString(dst, t.Name)
				dst = AppendCompactArrayLen(dst, len(t.PartitionIndexes), false)
				for _, p := range t.PartitionIndexes {
					dst = AppendInt32(dst, p)
				}
				dst = AppendEmptyTaggedFields(dst)
			}
		}
		dst = AppendEmptyTaggedFields(dst)
	}
	if r.RequireStable {
		dst = append(dst, 1)
	} else {
		dst = append(dst, 0)
	}
	dst = AppendEmptyTaggedFields(dst)
	return dst
}

// OffsetFetchResponse (v8).
type OffsetFetchResponse struct {
	ThrottleTimeMs int32
	Groups         []OffsetFetchGroupResp
}

type OffsetFetchGroupResp struct {
	GroupID   string
	Topics    []OffsetFetchTopicResp
	ErrorCode int16
}

type OffsetFetchTopicResp struct {
	Name       string
	Partitions []OffsetFetchPartitionResp
}

type OffsetFetchPartitionResp struct {
	PartitionIndex       int32
	CommittedOffset      int64
	CommittedLeaderEpoch int32
	Metadata             string
	MetadataNull         bool
	ErrorCode            int16
}

func DecodeOffsetFetchResponse(body []byte) (OffsetFetchResponse, error) {
	c := NewCursor(body)
	r := OffsetFetchResponse{}
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
		r.Groups = make([]OffsetFetchGroupResp, n)
		for i := range n {
			g := OffsetFetchGroupResp{}
			g.GroupID, err = c.ReadCompactString()
			if err != nil {
				return r, err
			}
			tn, _, err := c.ReadCompactArrayLen()
			if err != nil {
				return r, err
			}
			if tn > 0 {
				g.Topics = make([]OffsetFetchTopicResp, tn)
				for j := range tn {
					t := OffsetFetchTopicResp{}
					t.Name, err = c.ReadCompactString()
					if err != nil {
						return r, err
					}
					pn, _, err := c.ReadCompactArrayLen()
					if err != nil {
						return r, err
					}
					if pn > 0 {
						t.Partitions = make([]OffsetFetchPartitionResp, pn)
						for k := range pn {
							p := OffsetFetchPartitionResp{}
							if p.PartitionIndex, err = c.ReadInt32(); err != nil {
								return r, err
							}
							if p.CommittedOffset, err = c.ReadInt64(); err != nil {
								return r, err
							}
							if p.CommittedLeaderEpoch, err = c.ReadInt32(); err != nil {
								return r, err
							}
							p.Metadata, p.MetadataNull, err = c.ReadCompactNullableString()
							if err != nil {
								return r, err
							}
							if p.ErrorCode, err = c.ReadInt16(); err != nil {
								return r, err
							}
							if err := c.SkipTaggedFields(); err != nil {
								return r, err
							}
							t.Partitions[k] = p
						}
					}
					if err := c.SkipTaggedFields(); err != nil {
						return r, err
					}
					g.Topics[j] = t
				}
			}
			if g.ErrorCode, err = c.ReadInt16(); err != nil {
				return r, err
			}
			if err := c.SkipTaggedFields(); err != nil {
				return r, err
			}
			r.Groups[i] = g
		}
	}
	if err := c.SkipTaggedFields(); err != nil {
		return r, err
	}
	return r, nil
}
