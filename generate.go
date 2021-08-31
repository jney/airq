package airq

//go:generate msgpackgen -strict
//go:generate protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative job/job.proto
