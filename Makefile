msgpack:
	# doesn't work. get.. or install or whatever. anyway it worked once.
	go install github.com/shamaton/msgpackgen
	vendor/github.com/shamaton/msgpackgen/msgpackgen -strict

protoc:
	cd job; protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative job.proto