package flow

import "github.com/itchyny/gojq"

/*
 * Filter root
 */

type FilterRoot struct {
	group_filter *gojq.Code
	groups       map[string]*GroupNode
}

func NewFilterTree(group_filter *gojq.Code) *FilterRoot {
	return &FilterRoot{
		group_filter: group_filter,
		groups:       make(map[string]*GroupNode),
	}
}

func (r *FilterRoot) HasGroup(group string) bool {
	_, ok := r.groups[group]
	return ok
}

func (r *FilterRoot) AddGroup(group string, node *GroupNode) {
	r.groups[group] = node
}

func (r *FilterRoot) GetGroup(group string) *GroupNode {
	return r.groups[group]
}

/*
 * GroupNode
 */

type GroupNode struct {
	name string
	//group_filter *gojq.Code
	children []*LeafNode
}

func NewGroupNode(name string /*, group_filter *gojq.Code*/) *GroupNode {
	return &GroupNode{
		name: name,
		//group_filter: group_filter,
		children: make([]*LeafNode, 0),
	}
}

func (gf *GroupNode) AddChild(leaf *LeafNode) {
	gf.children = append(gf.children, leaf)
}

/*
 * LeafNode
 */

type LeafNode struct {
	Filter *gojq.Code
}
