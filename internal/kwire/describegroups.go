package kwire

// DescribeGroups v5 codec — kproxy is the client. v5 is the first flexible
// version and is sufficient for telemetry: we read group state, members, and
// member metadata/assignment blobs.

const DescribeGroupsVersion int16 = 5

// DescribeGroupsRequest (v5).
type DescribeGroupsRequest struct {
	Groups                      []string
	IncludeAuthorizedOperations bool
}

func AppendDescribeGroupsRequest(dst []byte, r DescribeGroupsRequest) []byte {
	dst = AppendCompactArrayLen(dst, len(r.Groups), false)
	for _, g := range r.Groups {
		dst = AppendCompactString(dst, g)
	}
	if r.IncludeAuthorizedOperations {
		dst = append(dst, 1)
	} else {
		dst = append(dst, 0)
	}
	dst = AppendEmptyTaggedFields(dst)
	return dst
}

// DescribeGroupsResponse (v5).
type DescribeGroupsResponse struct {
	ThrottleTimeMs int32
	Groups         []DescribedGroup
}

type DescribedGroup struct {
	ErrorCode            int16
	GroupID              string
	GroupState           string
	ProtocolType         string
	ProtocolData         string
	Members              []DescribedGroupMember
	AuthorizedOperations int32
}

type DescribedGroupMember struct {
	MemberID            string
	GroupInstanceID     string
	GroupInstanceIDNull bool
	ClientID            string
	ClientHost          string
	MemberMetadata      []byte // ConsumerProtocolSubscription blob; aliases input
	MemberAssignment    []byte // ConsumerProtocolAssignment blob; aliases input
}

func DecodeDescribeGroupsResponse(body []byte) (DescribeGroupsResponse, error) {
	c := NewCursor(body)
	r := DescribeGroupsResponse{}
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
		r.Groups = make([]DescribedGroup, n)
		for i := 0; i < n; i++ {
			g := DescribedGroup{}
			if g.ErrorCode, err = c.ReadInt16(); err != nil {
				return r, err
			}
			if g.GroupID, err = c.ReadCompactString(); err != nil {
				return r, err
			}
			if g.GroupState, err = c.ReadCompactString(); err != nil {
				return r, err
			}
			if g.ProtocolType, err = c.ReadCompactString(); err != nil {
				return r, err
			}
			if g.ProtocolData, err = c.ReadCompactString(); err != nil {
				return r, err
			}
			mn, _, err := c.ReadCompactArrayLen()
			if err != nil {
				return r, err
			}
			if mn > 0 {
				g.Members = make([]DescribedGroupMember, mn)
				for j := 0; j < mn; j++ {
					m := DescribedGroupMember{}
					if m.MemberID, err = c.ReadCompactString(); err != nil {
						return r, err
					}
					m.GroupInstanceID, m.GroupInstanceIDNull, err = c.ReadCompactNullableString()
					if err != nil {
						return r, err
					}
					if m.ClientID, err = c.ReadCompactString(); err != nil {
						return r, err
					}
					if m.ClientHost, err = c.ReadCompactString(); err != nil {
						return r, err
					}
					if m.MemberMetadata, err = c.ReadCompactBytes(); err != nil {
						return r, err
					}
					if m.MemberAssignment, err = c.ReadCompactBytes(); err != nil {
						return r, err
					}
					if err := c.SkipTaggedFields(); err != nil {
						return r, err
					}
					g.Members[j] = m
				}
			}
			if g.AuthorizedOperations, err = c.ReadInt32(); err != nil {
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
