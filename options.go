package main

import (
	"fmt"
	"io/ioutil"
	"os"

	"github.com/mongodb/mongo-tools/common/options"
	"github.com/pkg/errors"
	"github.com/rockset/rockset-mongo/pkg/config"
	"gopkg.in/yaml.v2"
)

var Usage = `<options> <connection-string>

Export the content of a Mongo collection into .bson files.

Specify a database with -d and a collection with -c to only dump that database or collection.

Connection strings must begin with mongodb:// or mongodb+srv://.

The options match mongodump options documented in http://docs.mongodb.com/database-tools/mongodump/ for more information.`

// InputOptions defines the set of options to use in retrieving data from the server.
type InputOptions struct {
	Query          string `long:"query" short:"q" description:"query filter, as a v2 Extended JSON string, e.g., '{\"x\":{\"$gt\":1}}'"`
	QueryFile      string `long:"queryFile" description:"path to a file containing a query filter (v2 Extended JSON)"`
	ReadPreference string `long:"readPreference" value-name:"<string>|<json>" description:"specify either a preference mode (e.g. 'nearest') or a preference json object (e.g. '{mode: \"nearest\", tagSets: [{a: \"b\"}], maxStalenessSeconds: 123}')"`
}

// Name returns a human-readable group name for input options.
func (*InputOptions) Name() string {
	return "query"
}

func (inputOptions *InputOptions) HasQuery() bool {
	return inputOptions.Query != "" || inputOptions.QueryFile != ""
}

func (inputOptions *InputOptions) GetQuery() ([]byte, error) {
	if inputOptions.Query != "" {
		return []byte(inputOptions.Query), nil
	} else if inputOptions.QueryFile != "" {
		content, err := ioutil.ReadFile(inputOptions.QueryFile)
		if err != nil {
			err = fmt.Errorf("error reading queryFile: %s", err)
		}
		return content, err
	}
	panic("GetQuery can return valid values only for query or queryFile input")
}

// OutputOptions defines the set of options for writing dump data.
type OutputOptions struct {
	Out        string `long:"out" default:"dump" value-name:"<directory-path>" short:"o" description:"output directory, or '-' for stdout (default: 'dump')"`
	Gzip       bool   `long:"gzip" description:"compress collection output with Gzip"`
	TargetSize uint32 `long:"target_size" default:"100" description:"target file size in MiB"`
}

// Name returns a human-readable group name for output options.
func (*OutputOptions) Name() string {
	return "output"
}

type Options struct {
	*options.ToolOptions
	*InputOptions
	*OutputOptions
}

func ParseOptions(rawArgs []string, versionStr, gitCommit string) (Options, error) {
	opts := options.New("rockset-mongodump", versionStr, gitCommit, Usage, true, options.EnabledOptions{Auth: true, Connection: true, Namespace: true, URI: true})

	inputOpts := &InputOptions{}
	opts.AddOptions(inputOpts)
	outputOpts := &OutputOptions{}
	opts.AddOptions(outputOpts)

	extraArgs, err := ParseArgs(opts, rawArgs)
	if err != nil {
		return Options{}, err
	}

	if len(extraArgs) > 0 {
		return Options{}, fmt.Errorf("error parsing positional arguments: " +
			"provide only one MongoDB connection string. " +
			"Connection strings must begin with mongodb:// or mongodb+srv:// schemes",
		)
	}

	return Options{opts, inputOpts, outputOpts}, nil
}

func ParseArgs(opts *options.ToolOptions, args []string) ([]string, error) {
	options.LogSensitiveOptionWarnings(args)

	if err := ParseConfigFile(opts, args); err != nil {
		return []string{}, err
	}

	args, err := opts.CallArgParser(args)
	if err != nil {
		return []string{}, err
	}

	err = opts.NormalizeOptionsAndURI()
	if err != nil {
		return []string{}, err
	}

	return args, err
}

func ParseConfigFile(opts *options.ToolOptions, args []string) error {
	// Get config file path from the arguments, if specified.
	_, err := opts.CallArgParser(args)
	if err != nil {
		return err
	}

	// No --config option was specified.
	if opts.General.ConfigPath == "" {
		return nil
	}

	// --config option siputils a file path.
	configBytes, err := os.ReadFile(opts.General.ConfigPath)
	if err != nil {
		return errors.Wrapf(err, "error opening file with --config")
	}

	// Unmarshal the config file as a top-level YAML file.
	var config config.Config
	err = yaml.UnmarshalStrict(configBytes, &config)
	if err != nil {
		return errors.Wrapf(err, "error parsing config file %s", opts.General.ConfigPath)
	}

	// Assign each parsed value to its respective ToolOptions field.
	opts.URI.ConnectionString = config.Mongo.Uri

	//// Mongomirror has an extra option to set.
	//for _, extraOpt := range opts.URI.extraOptionsRegistry {
	//if destinationAuth, ok := extraOpt.(DestinationAuthOptions); ok {
	//destinationAuth.SetDestinationPassword(config.DestinationPassword)
	//break
	//}
	//}

	return nil
}
