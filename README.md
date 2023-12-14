# Rockset-mongo

Helper tool for onboarding very large MongoDB collections into Rockset, targeting collections exceeding 200GiB. It uses Rockset's highly scalable S3 connector to accelerate the initial load, then sets up Rockset to stream data continuously from Rockset directly.

## How to run

### 1. Setup an S3 bucket and an S3 integration

This tool accelerates initial load by exporting MongoDB data directly to S3, then Rockset would ingest the exported files in parallel.

You will need to set up an S3 integration along with IAM Role/Policy to allow Rockset access. You can follow the [Create an S3 integration](https://docs.rockset.com/documentation/docs/amazon-s3#create-an-s3-integration) guide for that.

### 2. Setup an MongoDB integration

After initial load completes, Rockset will connect to the MongoDB database instance directly to monitor MongoDB for updates.

Follow the [Create a MongoDB Atlas Integration](https://docs.rockset.com/documentation/docs/mongodb-atlas#create-a-mongodb-atlas-integration) or [Create a Self-Managed MongoDB Integration](https://docs.rockset.com/documentation/docs/mongodb-self-managed) to prepare Rockset to create the integration.

### 3. Create Rockset API Key

This tool uses the Rockset API, and will require API Key access.

Create a new API Key with `member` role, with permissions to create collections using the integrations above, at https://console.dev.rockset.com/apikeys .

### 4. Export!

You are almost ready to create the collection. Create a configuration file detailing above configuration. The configuration details all necessary to create the collection:

```yaml
rockset_collection: commons.mongotest13           # Rockset collection name

rockset:
  api_key: Mea..                                  # Rockset API key to use to create collection
  api_server: https://api.usw2a1.dev.rockset.com/ # api server to use.

mongo:
  uri: mongodb+srv://username:password@foobar.14l6aa6.mongodb.net/ # URI for MongoDB collection
  db: sample_training               # MongoDB Database
  collection: companies				# MongoDB collection
  integration: prod-mongo           # Rockset integration configured with MongoDB credentials

s3:
  uri: s3://export-bucket/prefix    # S3 URI where MongodB collection will be exported to
  integration: s3-runbook-example   # Rockset integration with read access to the S3 path

create_collection_request:
  # create request specification, containing transformation info, retention, etc,
  # See https://docs.rockset.com/documentation/reference/createcollection
  # for all the options, but without sources
  field_mapping_query:
    sql: |
      SELECT *
      EXCEPT (_meta)
      FROM _input
  storage_compression_type: LZ4
```

Then you can run:

```bash
./rockset-mongo --config ./config.yaml
```

Note that `rockset-mongo` uses AWS Credentials specified in the local environment, similar to `aws` CLI tool.

## Notes

* You can use a different read-replica for the initial export to avoid overwhelming the production database. The configuration file can use a mongo URI that's different from the MongoDB integrations.
* You can pass `--load-only` if only initial is needed. You can skip Rockset MongoDB integration, and specifiying `mongo.integration` field

## Caveats/Known issues

This is a beta tool now and has some rough edges:

* The tool writes a local `state.json` and `s3buffer` directory. They need to be cleared before exporting new collections.