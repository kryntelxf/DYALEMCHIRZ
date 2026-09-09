module github.com/kryntelxf/DYALEMCHIRZ

go 1.21

require (
	github.com/kryntelxf/DYALEMCHIRZ/dya v0.0.0
	k8s.io/api v0.29.0
	k8s.io/apimachinery v0.29.0
	k8s.io/client-go v0.29.0
	k8s.io/klog/v2 v2.120.1
)

replace github.com/kryntelxf/DYALEMCHIRZ/dya => ./dya
