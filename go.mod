module github.com/wuyazi/gddd

go 1.16

require (
	github.com/apache/rocketmq-client-go/v2 v2.1.0
	github.com/bwmarrin/snowflake v0.3.0
	github.com/jinzhu/copier v0.3.5
	github.com/sirupsen/logrus v1.8.1 // indirect
	github.com/stretchr/testify v1.7.0 // indirect
	github.com/valyala/bytebufferpool v1.0.0
	golang.org/x/sys v0.0.0-20211216021012-1d35b9e2eb4e // indirect
	gopkg.in/yaml.v3 v3.0.0-20210107192922-496545a6307b // indirect
)

replace github.com/apache/rocketmq-client-go/v2 => ../../../github.com/wuyazi/rocketmq-client-go
