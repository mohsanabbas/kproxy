package kwire

// FindCoordinatorResponse is the decoded form of FindCoordinatorResponse v0..6.
//
// Schema reference: Apache Kafka 3.9 FindCoordinatorResponse.json.
//
//	v0:    ErrorCode, NodeID, Host, Port
//	v1+:   + ThrottleTimeMs (prepended), + ErrorMessage (after ErrorCode)
//	v3+:   flexible
//	v4+:   schema split: top-level NodeID/Host/Port and ErrorMessage gone;
//	       replaced by Coordinators[] (one entry per requested key).
//	v5+:   no field changes (added new error code)
//	v6+:   no field changes (share-groups support)
//
// We keep both shapes in one struct: the v0..3 fields are populated when
// Version <= 3, and Coordinators is populated when Version >= 4.
type FindCoordinatorResponse struct {
	Version        int16
	ThrottleTimeMs int32 // v1+

	// v0..v3 single-coordinator shape
	ErrorCode        int16
	ErrorMessage     string // v1..v3
	ErrorMessageNull bool
	NodeID           int32
	Host             string
	Port             int32

	// v4+ multi-coordinator shape
	Coordinators []FindCoordinatorEntry
}

type FindCoordinatorEntry struct {
	Key              string
	NodeID           int32
	Host             string
	Port             int32
	ErrorCode        int16
	ErrorMessage     string
	ErrorMessageNull bool
}

// DecodeFindCoordinatorResponse decodes a FindCoordinator response body.
func DecodeFindCoordinatorResponse(body []byte, version int16) (FindCoordinatorResponse, error) {
	c := NewCursor(body)
	r := FindCoordinatorResponse{Version: version}
	flex := IsFlexibleResponse(APIFindCoordinator, version)

	if version >= 1 {
		v, err := c.ReadInt32()
		if err != nil {
			return r, err
		}
		r.ThrottleTimeMs = v
	}

	if version <= 3 {
		ec, err := c.ReadInt16()
		if err != nil {
			return r, err
		}
		r.ErrorCode = ec
		if version >= 1 {
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
			r.ErrorMessage = s
			r.ErrorMessageNull = isNull
		}
		if r.NodeID, err = c.ReadInt32(); err != nil {
			return r, err
		}
		if flex {
			r.Host, err = c.ReadCompactString()
		} else {
			r.Host, err = c.ReadString()
		}
		if err != nil {
			return r, err
		}
		if r.Port, err = c.ReadInt32(); err != nil {
			return r, err
		}
	} else {
		// v4+
		n, err := readArrLen(&c, flex)
		if err != nil {
			return r, err
		}
		if n > 0 {
			r.Coordinators = make([]FindCoordinatorEntry, n)
			for i := range n {
				e, err := decodeFindCoordEntry(&c, flex)
				if err != nil {
					return r, err
				}
				r.Coordinators[i] = e
			}
		}
	}
	if flex {
		if err := c.SkipTaggedFields(); err != nil {
			return r, err
		}
	}
	return r, nil
}

// AppendFindCoordinatorResponse encodes a FindCoordinator response body.
func AppendFindCoordinatorResponse(dst []byte, r FindCoordinatorResponse) []byte {
	flex := IsFlexibleResponse(APIFindCoordinator, r.Version)
	if r.Version >= 1 {
		dst = AppendInt32(dst, r.ThrottleTimeMs)
	}
	if r.Version <= 3 {
		dst = AppendInt16(dst, r.ErrorCode)
		if r.Version >= 1 {
			if flex {
				dst = AppendCompactNullableString(dst, r.ErrorMessage, r.ErrorMessageNull)
			} else {
				dst = AppendNullableString(dst, r.ErrorMessage, r.ErrorMessageNull)
			}
		}
		dst = AppendInt32(dst, r.NodeID)
		if flex {
			dst = AppendCompactString(dst, r.Host)
		} else {
			dst = AppendString(dst, r.Host)
		}
		dst = AppendInt32(dst, r.Port)
	} else {
		dst = appendArrLen(dst, len(r.Coordinators), flex)
		for i := range r.Coordinators {
			dst = appendFindCoordEntry(dst, r.Coordinators[i], flex)
		}
	}
	if flex {
		dst = AppendEmptyTaggedFields(dst)
	}
	return dst
}

func decodeFindCoordEntry(c *Cursor, flex bool) (FindCoordinatorEntry, error) {
	e := FindCoordinatorEntry{}
	var err error
	if flex {
		e.Key, err = c.ReadCompactString()
	} else {
		e.Key, err = c.ReadString()
	}
	if err != nil {
		return e, err
	}
	if e.NodeID, err = c.ReadInt32(); err != nil {
		return e, err
	}
	if flex {
		e.Host, err = c.ReadCompactString()
	} else {
		e.Host, err = c.ReadString()
	}
	if err != nil {
		return e, err
	}
	if e.Port, err = c.ReadInt32(); err != nil {
		return e, err
	}
	if e.ErrorCode, err = c.ReadInt16(); err != nil {
		return e, err
	}
	var isNull bool
	if flex {
		e.ErrorMessage, isNull, err = c.ReadCompactNullableString()
	} else {
		e.ErrorMessage, isNull, err = c.ReadNullableString()
	}
	if err != nil {
		return e, err
	}
	e.ErrorMessageNull = isNull
	if flex {
		if err := c.SkipTaggedFields(); err != nil {
			return e, err
		}
	}
	return e, nil
}

func appendFindCoordEntry(dst []byte, e FindCoordinatorEntry, flex bool) []byte {
	if flex {
		dst = AppendCompactString(dst, e.Key)
	} else {
		dst = AppendString(dst, e.Key)
	}
	dst = AppendInt32(dst, e.NodeID)
	if flex {
		dst = AppendCompactString(dst, e.Host)
	} else {
		dst = AppendString(dst, e.Host)
	}
	dst = AppendInt32(dst, e.Port)
	dst = AppendInt16(dst, e.ErrorCode)
	if flex {
		dst = AppendCompactNullableString(dst, e.ErrorMessage, e.ErrorMessageNull)
		dst = AppendEmptyTaggedFields(dst)
	} else {
		dst = AppendNullableString(dst, e.ErrorMessage, e.ErrorMessageNull)
	}
	return dst
}
