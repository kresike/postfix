/*Package postfix is a data handler package for the Postfix mail server */
package postfix

/*Policy is the main data type for storing attributes of the postfix policy protocol */
type Policy struct {
	attributes map[string]string
}

/*NewPolicy constructor for initializing the Policy structure with a map inside */
func NewPolicy() *Policy {
	var p Policy
	p.attributes = make(map[string]string)
	return &p
}

/*SetAttribute is a setter function for the Policy structure */
func (pp Policy) SetAttribute(key, value string) {
	pp.attributes[key] = value
}

/*Attribute is a getter function for the Policy structure */
func (pp Policy) Attribute(key string) string {
	return pp.attributes[key]
}

/*Keys lists the names of the attributes currently stored in a Policy structure */
func (pp Policy) Keys() []string {
	keys := make([]string, 0, len(pp.attributes))
	for k := range pp.attributes {
		keys = append(keys, k)
	}
	return keys
}

/*String is a simple stringer, it only displays attributes that have an actual value */
func (pp Policy) String() string {
	res := "Postfix policy attributes:\n"
	for _, k := range pp.Keys() {
		a := pp.Attribute(k)
		if a != "" {
			res += k + ": " + pp.Attribute(k) + "\n"
		}
	}
	return res
}
