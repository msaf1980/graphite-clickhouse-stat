package main

type stringSet map[string]struct{}

func newStringsSet(vals []string) stringSet {
	m := make(map[string]struct{})
	for _, v := range vals {
		m[v] = struct{}{}
	}
	return m
}

func (m stringSet) Exist(v string) bool {
	_, ok := m[v]
	return ok
}
