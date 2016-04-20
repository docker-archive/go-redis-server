/*
 Warning: a very unsophisticated, i.e., unefficent ordered set implementation.
 Should make use of binary search in the future, e.g., sort.Search.
*/

package redis

type OrderedSet struct {
	index    map[string]bool
	elements []orderedSetElement
}

type orderedSetElement struct {
	score int
	value []byte
}

func NewOrderedSet() *OrderedSet {
	newSet := OrderedSet{
		index:    map[string]bool{},
		elements: []orderedSetElement{},
	}
	return &newSet
}

func (self *OrderedSet) Add(score int, value []byte) int {
	if _, ok := self.index[string(value)]; ok {
		return 0
	}
	self.index[string(value)] = true

	addIndex := 0
	for i, e := range self.elements {
		if score < e.score {
			addIndex = i
			break
		} else if i == len(self.elements)-1 {
			addIndex = i + 1
		}
	}

	newElement := orderedSetElement{
		score: score,
		value: value,
	}

	self.elements = append(
		self.elements[:addIndex], append(
			[]orderedSetElement{newElement}, self.elements[addIndex:]...)...)

	return 1
}

func (self *OrderedSet) Range(lowerIndex, upperIndex int) [][]byte {
	result := [][]byte{}

	lower, upper, ok := self.lowerAndUpperFromIndexes(lowerIndex, upperIndex)
	if !ok {
		return result
	}

	for _, e := range self.elements[lower:upper] {
		result = append(result, e.value)
	}

	return result
}

func (self *OrderedSet) RangeByScore(lowerScore, upperScore int) [][]byte {
	result := [][]byte{}

	lower, upper, ok := self.lowerAndUpperFromScores(lowerScore, upperScore)
	if !ok {
		return result
	}

	for _, e := range self.elements[lower:upper] {
		result = append(result, e.value)
	}

	return result
}

func (self *OrderedSet) Rem(value []byte) int {
	if _, ok := self.index[string(value)]; !ok {
		return 0
	}

	delete(self.index, string(value))

	remIndex := 0
	for i, e := range self.elements {
		if string(e.value) == string(value) {
			remIndex = i
			break
		}
	}

	self.elements = append(self.elements[:remIndex], self.elements[remIndex+1:]...)

	return 1
}

func (self *OrderedSet) RemRangeByScore(lowerScore, upperScore int) int {
	lower, upper, ok := self.lowerAndUpperFromScores(lowerScore, upperScore)
	if !ok {
		return 0
	}

	for _, e := range self.elements[lower:upper] {
		delete(self.index, string(e.value))
	}

	length := len(self.elements)

	self.elements = append(self.elements[:lower], self.elements[upper:]...)

	return length - len(self.elements)
}

func (self *OrderedSet) lowerAndUpperFromIndexes(lowerIndex, upperIndex int) (int, int, bool) {
	lower := 0
	upper := 0

	if lowerIndex < 0 {
		lower = len(self.elements) + 1 + lowerIndex
	}

	if upperIndex < 0 {
		upper = len(self.elements) + 1 + upperIndex
	} else if len(self.elements) <= upperIndex {
		upper = len(self.elements)
	}

	if len(self.elements) <= lower || upper < lower {
		return 0, 0, false
	}

	return lower, upper, true
}

func (self *OrderedSet) lowerAndUpperFromScores(lowerScore, upperScore int) (int, int, bool) {
	lower := 0
	upper := 0

	for i, e := range self.elements {
		if lowerScore >= e.score {
			lower = i + 1
		}
		if upperScore >= e.score {
			upper = i + 1
		}
	}

	if len(self.elements) <= lower || upper < lower {
		return 0, 0, false
	}

	return lower, upper, true
}
