package kwire

// SyncGroupRequest is the decoded form of SyncGroup. Field availability by
// version follows the Kafka schema:
//
//	v0..v2: Group, Generation, MemberID, Assignments
//	v3:    + InstanceID
//	v4:    flexible header + tagged fields
//	v5:    + ProtocolType, ProtocolName
//
// Fields not present in the encoded version are left zero / nil.
type SyncGroupRequest struct {
	Version      int16
	Group        string
	Generation   int32
	MemberID     string
	InstanceID   string // v3+
	InstancePres bool   // tracks present-vs-null at protocol level
	InstanceNull bool
	ProtocolType string // v5+
	ProtoTypePres bool
	ProtoTypeNull bool
	ProtocolName string // v5+
	ProtoNamePres bool
	ProtoNameNull bool
	Assignments  []SyncGroupAssignment
}

// SyncGroupAssignment is one entry in SyncGroupRequest.Assignments.
type SyncGroupAssignment struct {
	MemberID         string
	MemberAssignment []byte // ConsumerProtocolAssignment blob; aliases input
	MAIsNull         bool
}

// DecodeSyncGroupRequest decodes a SyncGroup request body (the bytes following
// the request header).
func DecodeSyncGroupRequest(body []byte, version int16) (SyncGroupRequest, error) {
	c := NewCursor(body)
	r := SyncGroupRequest{Version: version}
	flex := IsFlexibleRequest(APISyncGroup, version)

	var err error
	if flex {
		r.Group, err = c.ReadCompactString()
	} else {
		r.Group, err = c.ReadString()
	}
	if err != nil {
		return r, err
	}
	if r.Generation, err = c.ReadInt32(); err != nil {
		return r, err
	}
	if flex {
		r.MemberID, err = c.ReadCompactString()
	} else {
		r.MemberID, err = c.ReadString()
	}
	if err != nil {
		return r, err
	}
	if version >= 3 {
		var isNull bool
		if flex {
			r.InstanceID, isNull, err = c.ReadCompactNullableString()
		} else {
			r.InstanceID, isNull, err = c.ReadNullableString()
		}
		if err != nil {
			return r, err
		}
		r.InstancePres = true
		r.InstanceNull = isNull
	}
	if version >= 5 {
		var isNull bool
		if flex {
			r.ProtocolType, isNull, err = c.ReadCompactNullableString()
		} else {
			r.ProtocolType, isNull, err = c.ReadNullableString()
		}
		if err != nil {
			return r, err
		}
		r.ProtoTypePres = true
		r.ProtoTypeNull = isNull
		if flex {
			r.ProtocolName, isNull, err = c.ReadCompactNullableString()
		} else {
			r.ProtocolName, isNull, err = c.ReadNullableString()
		}
		if err != nil {
			return r, err
		}
		r.ProtoNamePres = true
		r.ProtoNameNull = isNull
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
		r.Assignments = make([]SyncGroupAssignment, n)
		for i := 0; i < n; i++ {
			var a SyncGroupAssignment
			if flex {
				a.MemberID, err = c.ReadCompactString()
			} else {
				a.MemberID, err = c.ReadString()
			}
			if err != nil {
				return r, err
			}
			if flex {
				a.MemberAssignment, a.MAIsNull, err = c.ReadCompactNullableBytes()
			} else {
				a.MemberAssignment, a.MAIsNull, err = c.ReadNullableBytes()
			}
			if err != nil {
				return r, err
			}
			if flex {
				if err := c.SkipTaggedFields(); err != nil { // assignment-level tagged
					return r, err
				}
			}
			r.Assignments[i] = a
		}
	}
	if flex {
		if err := c.SkipTaggedFields(); err != nil { // request-level tagged
			return r, err
		}
	}
	return r, nil
}

// AppendSyncGroupRequest serialises a SyncGroup request body using its Version.
func AppendSyncGroupRequest(dst []byte, r SyncGroupRequest) []byte {
	flex := IsFlexibleRequest(APISyncGroup, r.Version)

	if flex {
		dst = AppendCompactString(dst, r.Group)
	} else {
		dst = AppendString(dst, r.Group)
	}
	dst = AppendInt32(dst, r.Generation)
	if flex {
		dst = AppendCompactString(dst, r.MemberID)
	} else {
		dst = AppendString(dst, r.MemberID)
	}
	if r.Version >= 3 {
		if flex {
			dst = AppendCompactNullableString(dst, r.InstanceID, r.InstanceNull)
		} else {
			dst = AppendNullableString(dst, r.InstanceID, r.InstanceNull)
		}
	}
	if r.Version >= 5 {
		if flex {
			dst = AppendCompactNullableString(dst, r.ProtocolType, r.ProtoTypeNull)
			dst = AppendCompactNullableString(dst, r.ProtocolName, r.ProtoNameNull)
		} else {
			dst = AppendNullableString(dst, r.ProtocolType, r.ProtoTypeNull)
			dst = AppendNullableString(dst, r.ProtocolName, r.ProtoNameNull)
		}
	}

	if flex {
		dst = AppendCompactArrayLen(dst, len(r.Assignments), false)
	} else {
		dst = AppendArrayLen(dst, len(r.Assignments), false)
	}
	for _, a := range r.Assignments {
		if flex {
			dst = AppendCompactString(dst, a.MemberID)
			dst = AppendCompactNullableBytes(dst, a.MemberAssignment, a.MAIsNull)
			dst = AppendEmptyTaggedFields(dst)
		} else {
			dst = AppendString(dst, a.MemberID)
			dst = AppendNullableBytes(dst, a.MemberAssignment, a.MAIsNull)
		}
	}
	if flex {
		dst = AppendEmptyTaggedFields(dst)
	}
	return dst
}

// SyncGroupResponse is the decoded form of a SyncGroup response.
//
//	v0..v0: ErrorCode, MemberAssignment
//	v1:    + ThrottleTimeMs (prepended)
//	v4:    flexible
//	v5:    + ProtocolType, ProtocolName (after MemberID-related fields)
type SyncGroupResponse struct {
	Version          int16
	ThrottleTimeMs   int32 // v1+
	ErrorCode        int16
	ProtocolType     string // v5+
	ProtoTypeNull    bool
	ProtocolName     string // v5+
	ProtoNameNull    bool
	MemberAssignment []byte
	MAIsNull         bool
}

// DecodeSyncGroupResponse decodes a SyncGroup response body.
func DecodeSyncGroupResponse(body []byte, version int16) (SyncGroupResponse, error) {
	c := NewCursor(body)
	r := SyncGroupResponse{Version: version}
	flex := IsFlexibleResponse(APISyncGroup, version)

	var err error
	if version >= 1 {
		if r.ThrottleTimeMs, err = c.ReadInt32(); err != nil {
			return r, err
		}
	}
	if r.ErrorCode, err = c.ReadInt16(); err != nil {
		return r, err
	}
	if version >= 5 {
		var isNull bool
		if flex {
			r.ProtocolType, isNull, err = c.ReadCompactNullableString()
		} else {
			r.ProtocolType, isNull, err = c.ReadNullableString()
		}
		if err != nil {
			return r, err
		}
		r.ProtoTypeNull = isNull
		if flex {
			r.ProtocolName, isNull, err = c.ReadCompactNullableString()
		} else {
			r.ProtocolName, isNull, err = c.ReadNullableString()
		}
		if err != nil {
			return r, err
		}
		r.ProtoNameNull = isNull
	}
	if flex {
		r.MemberAssignment, r.MAIsNull, err = c.ReadCompactNullableBytes()
	} else {
		r.MemberAssignment, r.MAIsNull, err = c.ReadNullableBytes()
	}
	if err != nil {
		return r, err
	}
	if flex {
		if err := c.SkipTaggedFields(); err != nil {
			return r, err
		}
	}
	return r, nil
}

// AppendSyncGroupResponse serialises a SyncGroup response body.
func AppendSyncGroupResponse(dst []byte, r SyncGroupResponse) []byte {
	flex := IsFlexibleResponse(APISyncGroup, r.Version)
	if r.Version >= 1 {
		dst = AppendInt32(dst, r.ThrottleTimeMs)
	}
	dst = AppendInt16(dst, r.ErrorCode)
	if r.Version >= 5 {
		if flex {
			dst = AppendCompactNullableString(dst, r.ProtocolType, r.ProtoTypeNull)
			dst = AppendCompactNullableString(dst, r.ProtocolName, r.ProtoNameNull)
		} else {
			dst = AppendNullableString(dst, r.ProtocolType, r.ProtoTypeNull)
			dst = AppendNullableString(dst, r.ProtocolName, r.ProtoNameNull)
		}
	}
	if flex {
		dst = AppendCompactNullableBytes(dst, r.MemberAssignment, r.MAIsNull)
		dst = AppendEmptyTaggedFields(dst)
	} else {
		dst = AppendNullableBytes(dst, r.MemberAssignment, r.MAIsNull)
	}
	return dst
}
