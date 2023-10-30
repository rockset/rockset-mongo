package main

import (
	"context"
	"os"
	"time"

	"github.com/mongodb/mongo-tools/common/log"
	"github.com/mongodb/mongo-tools/common/progress"
	"github.com/mongodb/mongo-tools/common/signals"
	"github.com/mongodb/mongo-tools/common/util"
	"github.com/rockset/rockset-mongo/pkg/config"
	"github.com/rockset/rockset-mongo/pkg/mongo"
	"github.com/rockset/rockset-mongo/pkg/writers"
)

const (
	progressBarLength   = 50
	progressBarWaitTime = 3 * time.Second
)

var (
	VersionStr = "built-without-version-string"
	GitCommit  = "build-without-git-commit"
)

func main() {
	if os.Args[1] == "export" {
		export(os.Args[2:])
		return
	}

	run(os.Args[1:])
}

func run(args []string) {
	ctx, cancelFn := context.WithCancel(context.Background())
	defer cancelFn()
	finishedChan := signals.HandleWithInterrupt(cancelFn)
	defer close(finishedChan)

	opts, err := ParseOptions(args, VersionStr, GitCommit)
	if err != nil {
		log.Logvf(log.Always, "error parsing command line options: %s", err.Error())
		log.Logvf(log.Always, util.ShortUsage("mongodump"))
		os.Exit(util.ExitFailure)
	}

	// print help, if specified
	if opts.PrintHelp(false) {
		return
	}

	// print version, if specified
	if opts.PrintVersion() {
		return
	}
	log.SetVerbosity(opts.Verbosity)

	conf, err := config.ReadConfig(opts.ConfigPath)
	if err != nil {
		log.Logvf(log.Always, "error parsing config file: %v", err)
		os.Exit(util.ExitFailure)
	}

	state, err := config.ReadState("state.json")
	if err != nil {
		log.Logvf(log.Always, "error parsing state file: %v", err)
		os.Exit(util.ExitFailure)
	}

	// init logger
	d := &Driver{
		config:   conf,
		state:    state,
		dumpOpts: *opts.ToolOptions,

		tui:      false,
		logLevel: log.Always,
	}

	if err := d.run(ctx); err != nil {
		log.Logvf(log.Always, "error: %v", err)
		os.Exit(util.ExitFailure)
	}
}

func export(args []string) {
	ctx, cancelFn := context.WithCancel(context.Background())
	defer cancelFn()
	finishedChan := signals.HandleWithInterrupt(cancelFn)
	defer close(finishedChan)

	opts, err := ParseOptions(args, VersionStr, GitCommit)
	if err != nil {
		log.Logvf(log.Always, "error parsing command line options: %s", err.Error())
		log.Logvf(log.Always, util.ShortUsage("mongodump"))
		os.Exit(util.ExitFailure)
	}

	// print help, if specified
	if opts.PrintHelp(false) {
		return
	}

	// print version, if specified
	if opts.PrintVersion() {
		return
	}

	// init logger
	log.SetVerbosity(opts.Verbosity)

	// verify uri options and log them
	opts.URI.LogUnsupportedOptions()

	dump := mongo.MongoDump{
		ToolOptions: opts.ToolOptions,
	}

	if err = dump.Init(); err != nil {
		log.Logvf(log.Always, "Failed: %v", err)
		os.Exit(util.ExitFailure)
	}

	out, err := writers.NewWriter(ctx, &writers.WriterOptions{
		Out:             opts.Out,
		TargetChunkSize: uint64(opts.TargetSize),
		FilePrefix:      opts.ToolOptions.DB + "." + opts.ToolOptions.Collection,
	})
	if err != nil {
		log.Logvf(log.Always, "Failed to create writer: %v", err)
		os.Exit(util.ExitFailure)
	}
	defer out.Close()

	info, err := dump.CollectionInfo(ctx)
	if err != nil {
		log.Logvf(log.Always, "failed to get collection info: %v", err)
		os.Exit(util.ExitFailure)
	}

	progressManager := progress.NewBarWriter(log.Writer(0), progressBarWaitTime, progressBarLength, false)
	progressManager.Start()
	defer progressManager.Stop()

	dumpProgressor := progress.NewCounter(int64(info.Documents))
	dbNamespace := opts.DB + "." + opts.Collection
	if progressManager != nil {
		progressManager.Attach(dbNamespace, dumpProgressor)
		defer progressManager.Detach(dbNamespace)
	}

	dump.CollectionInfo(ctx)
	log.Logvf(log.Always, "exporting")
	if err = dump.Dump(ctx, out, dumpProgressor); err != nil {
		log.Logvf(log.Always, "Failed: %v", err)
		os.Exit(util.ExitFailure)
	}
}
