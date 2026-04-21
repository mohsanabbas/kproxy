package kwire

// JoinGroupRequest is the decoded form of JoinGroup.
//
//	v0:    Group, SessionTimeoutMs, MemberID, ProtocolType, Protocols
//	v1:    + RebalanceTimeoutMs (after SessionTimeoutMs)
//	v5:    + GroupInstanceID    (after MemberID)
//	v6:    flexible
//	v9:    + Reason             (after Protocols, NULLABLE_STRING)
type JoinGroupRequest struct {
	Version            int16
	Group              string
	SessionTimeoutMs   int32
	RebalanceTimeoutMs int32 // v1+; -1 sentinel pre-v1
	MemberID           string
	GroupInstanceID    string // v5+
	GIIDPresent        bool
	GIIDNull           bool
	ProtocolType       string
	Protocols          []JoinGroupProtocol
	Reason             string // v9+
	ReasonPresent      bool
	ReasonNull         bool
}

// JoinGroupProtocol is one entry in JoinGroupRequest.Protocols. The Metadata
// field is the encoded ConsumerProtocolSubscription blob (NOT decoded here).
type JoinGroupProtocol struct {
	Name     string
	Metadata []byte // aliases input
}

// DecodeJoinGroupRequest decodes a JoinGroup request body.
func DecodeJoinGroupRequest(body []byte, version int16) (JoinGroupRequest, error) {
	c := NewCursor(body)
	r := JoinGroupRequest{Version: version}
	flex := IsFlexibleRequest(APIJoinGroup, version)

	var err error
	if flex {
		r.Group, err = c.ReadCompactString()
	} else {
		r.Group, err = c.ReadString()
	}
	if err != nil {
		return r, err
	}
	if r.SessionTimeoutMs, err = c.ReadInt32(); err != nil {
		return r, err
	}
	if version >= 1 {
		if r.RebalanceTimeoutMs, err = c.ReadInt32(); err != nil {
			return r, err
		}
	} else {
		r.RebalanceTimeoutMs = -1
	}
	if flex {
		r.MemberID, err = c.ReadCompactString()
	} else {
		r.MemberID, err = c.ReadString()
	}
	if err != nil {
		return r, err
	}
	if version >= 5 {
		var isNull bool
		if flex {
			r.GroupInstanceID, isNull, err = c.ReadCompactNullableString()
		} else {
			r.GroupInstanceID, isNull, err = c.ReadNullableString()
		}
		if err != nil {
			return r, err
		}
		r.GIIDPresent = true
		r.GIIDNull = isNull
	}
	if flex {
		r.ProtocolType, err = c.ReadCompactString()
	} else {
		r.ProtocolType, err = c.ReadString()
	}
	if err != nil {
		return r, err
	}

	var n int
	if flex {
		n, _, err = c.ReadCompactArrayLen()
	} else {
		n, _, err = c.ReadArrayLen()
	}
	if err != nil {
		return r, err
	}
	if n > 0 {
		r.Protocols = make([]JoinGroupProtocol, n)
		for i := range n {
			var p JoinGroupProtocol
			if flex {
				p.Name, err = c.ReadCompactString()
			} else {
				p.Name, err = c.ReadString()
			}
			if err != nil {
				return r, err
			}
			if flex {
				p.Metadata, err = c.ReadCompactBytes()
			} else {
				p.Metadata, err = c.ReadBytes()
			}
			if err != nil {
				return r, err
			}
			if flex {
				if err := c.SkipTaggedFields(); err != nil {
					return r, err
				}
			}
			r.Protocols[i] = p
		}
	}
	if version >= 9 {
		var isNull bool
		if flex {
			r.Reason, isNull, err = c.ReadCompactNullableString()
		} else {
			r.Reason, isNull, err = c.ReadNullableString()
		}
		if err != nil {
			return r, err
		}
		r.ReasonPresent = true
		r.ReasonNull = isNull
	}
	if flex {
		if err := c.SkipTaggedFields(); err != nil {
			return r, err
		}
	}
	return r, nil
}

// AppendJoinGroupRequest serializes a JoinGroup request body.
func AppendJoinGroupRequest(dst []byte, r JoinGroupRequest) []byte {
	flex := IsFlexibleRequest(APIJoinGroup, r.Version)
	if flex {
		dst = AppendCompactString(dst, r.Group)
	} else {
		dst = AppendString(dst, r.Group)
	}
	dst = AppendInt32(dst, r.SessionTimeoutMs)
	if r.Version >= 1 {
		dst = AppendInt32(dst, r.RebalanceTimeoutMs)
	}
	if flex {
		dst = AppendCompactString(dst, r.MemberID)
	} else {
		dst = AppendString(dst, r.MemberID)
	}
	if r.Version >= 5 {
		if flex {
			dst = AppendCompactNullableString(dst, r.GroupInstanceID, r.GIIDNull)
		} else {
			dst = AppendNullableString(dst, r.GroupInstanceID, r.GIIDNull)
		}
	}
	if flex {
		dst = AppendCompactString(dst, r.ProtocolType)
	} else {
		dst = AppendString(dst, r.ProtocolType)
	}
	if flex {
		dst = AppendCompactArrayLen(dst, len(r.Protocols), false)
	} else {
		dst = AppendArrayLen(dst, len(r.Protocols), false)
	}
	for _, p := range r.Protocols {
		if flex {
			dst = AppendCompactString(dst, p.Name)
			dst = AppendCompactBytes(dst, p.Metadata)
			dst = AppendEmptyTaggedFields(dst)
		} else {
			dst = AppendString(dst, p.Name)
			dst = AppendBytes(dst, p.Metadata)
		}
	}
	if r.Version >= 9 {
		if flex {
			dst = AppendCompactNullableString(dst, r.Reason, r.ReasonNull)
		} else {
			dst = AppendNullableString(dst, r.Reason, r.ReasonNull)
		}
	}
	if flex {
		dst = AppendEmptyTaggedFields(dst)
	}
	return dst
}
