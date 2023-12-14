package main

import (
	"context"
	"fmt"
	"os"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/mongodb/mongo-tools/common/log"
	"github.com/mongodb/mongo-tools/common/progress"
	"github.com/mongodb/mongo-tools/common/signals"
	"github.com/mongodb/mongo-tools/common/util"
	"github.com/rockset/rockset-mongo/pkg/config"
	"github.com/rockset/rockset-mongo/pkg/mongo"
	"github.com/rockset/rockset-mongo/pkg/writers"
	"golang.org/x/sync/errgroup"
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

	var p *tea.Program
	if false {
		d.tui = true
		d.logLevel = log.DebugLow

		p = tea.NewProgram(&model{d}, tea.WithContext(ctx))
	}

	var errGrp errgroup.Group
	errGrp.Go(func() error {
		err := d.run(ctx)
		if p != nil {
			p.Quit()
		}
		return err
	})

	if p != nil {
		if _, err := p.Run(); err != nil {
			log.Logvf(log.Always, "error: %v", err)
			os.Exit(util.ExitFailure)
		}
		cancelFn()
	}
	if err := errGrp.Wait(); err != nil {
		log.Logvf(log.Always, "error: %v", err)
		os.Exit(util.ExitFailure)
	}
	fmt.Println("Collection created and import is done")
}

func export(args []string) {
	ctx, cancelFn := context.WithCancel(context.Background())
	defer cancelFn()
	finishedChan := signals.HandleWithInterrupt(cancelFn)
	defer close(finishedChan)

	opts, err := ParseOptions(args, VersionStr, GitCommit)
	if err != nil {
		log.Logvf(log.Always, "error parsing command line options: %s", err.Error())
		log.Logvf(log.Always, util.ShortUsage("rockset-mongo export"))
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
		TargetChunkSize: uint64(opts.TargetSize) * 1024 * 1024,
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
	log.Logvf(log.Always, "exporting MongoDB collection %v.%v", opts.ToolOptions.DB, opts.ToolOptions.Collection)
	if err = dump.Dump(ctx, out, dumpProgressor); err != nil {
		log.Logvf(log.Always, "Failed: %v", err)
		os.Exit(util.ExitFailure)
	}
	log.Logvf(log.Always, "export done")
}
