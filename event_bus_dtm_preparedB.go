package gddd

import (
	"fmt"
	"github.com/dtm-labs/client/dtmcli"
	"github.com/dtm-labs/client/dtmcli/dtmimp"
	"github.com/dtm-labs/client/dtmcli/logger"
	"github.com/dtm-labs/dtm-examples/dtmutil"
	"github.com/gin-gonic/gin"
)

const (
	BusiPort = 8081
)

var BusiConf dtmcli.DBConf

func BaseAddRoute(app *gin.Engine) {
	app.GET("/api/busi/QueryPreparedB", dtmutil.WrapHandler(func(c *gin.Context) interface{} {
		logger.Debugf("%s QueryPreparedB", c.Query("gid"))
		bb, err := dtmcli.BarrierFromQuery(c.Request.URL.Query())
		logger.FatalIfError(err)
		db := dtmutil.DbGet(BusiConf).ToSQLDB()
		return bb.QueryPrepared(db)
	}))
}

// Startup startup the busi's http service
func Startup(conf dtmimp.DBConf) *gin.Engine {
	BusiConf = conf
	app := dtmutil.GetGinApp()
	BaseAddRoute(app)
	return app
}

// RunHTTP will run http server
func RunHTTP(app *gin.Engine) {
	logger.Debugf("Starting busi at: %d", BusiPort)
	err := app.Run(fmt.Sprintf(":%d", BusiPort))
	dtmimp.FatalIfError(err)
}
