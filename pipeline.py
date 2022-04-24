import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.io.gcp.bigquery_tools import RetryStrategy
from apache_beam.io import ReadAllFromText
import argparse
import json

additional_bq_parameters = {
    "schemaUpdateOptions": ["ALLOW_FIELD_ADDITION","ALLOW_FIELD_RELAXATION"],
    "timePartitioning": (
        {
            "type": "DAY",
            "field": "ts",
            "requirePartitionFilter": True
        }
    ),
    "clustering":{
        "fields": ["event_name"]
    }
}

class ReadPubSubObject(beam.DoFn):
    def process(self, element, blob_path):
        pubsub_attribute = element.attributes
        if pubsub_attribute["eventType"] == "OBJECT_FINALIZE":
            # logging.info("--- blob_uri ---{}".format(blob_path.format(blob_name=pubsub_attribute["objectId"])))
            return [blob_path.format(blob_name=pubsub_attribute["objectId"])]
        return []

class ConvertDict(beam.DoFn):
    def process(self, element, *args, **kwargs):  
        from datetime import datetime
        import json
        
        element = json.loads(str(element))
        # logging.info("--- input element {}".format(element))
        def convert_dict(element):
            try:
                new_data = {}
                for k, v in element.items():
                    # Hard-coded to format "ts" field into desired timestramp string value for BigQuery
                    if k == "ts":
                        v = datetime.strptime(str(v),"%Y%m%d%H%M%S").strftime("%Y-%m-%d %H:%M:%S")
                        new_data[k] = v
                    if k == "eventName":
                        new_data["event_name"] = v
                    if k == "eventProps":
                        new_data["event_props"] = json.dumps(v)
                    if k == "deviceInfo":
                        new_data["device_info"] = json.dumps(v)
                    if k == "profile":
                        new_data[k] = json.dumps(v)
                    if k == "controlGroupName":
                        new_data["control_group_name"] = v
                    if k == "sessionProps":
                        new_data["session_props"] = json.dumps(v)
                return new_data
            except Exception as e:
                logging.info("-- Exception --{}".format(e))
                return {}
        output =  convert_dict(element)
        # logging.info("-- PRINT CONVERTED ELEMENT --{}".format(output))
        return [output]  

def run(argv=None, save_main_session=True):
    """Main entry point; defines and runs the pipeline"""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--pipeline_status",
        help="Flag whether it's development or not (prod)"
        )
    known_args, beam_args = parser.parse_known_args(argv)

    with open("job_config.json") as f:
        config = json.load(f)

    if known_args.pipeline_status=="dev":
        logging.info("Starting dev pipline")
        beam_options = PipelineOptions(
            beam_args,
            runner=config["dev"]["pipeline_options"]["runner"],
            project=config["dev"]["pipeline_options"]["project"],
            job_name=config["dev"]["pipeline_options"]["job_name"],
            streaming=config["dev"]["pipeline_options"]["streaming"],
            temp_location=config["dev"]["pipeline_options"]["temp_location"],
            region=config["dev"]["pipeline_options"]["region"],
            max_num_workers=config["dev"]["pipeline_options"]["max_num_workers"]
        )
        table = config["dev"]["output_table"]
    elif known_args.pipeline_status=="prod":
        logging.info("Starting prod pipline")
        beam_options = PipelineOptions(
            beam_args,
            runner=config["prod"]["pipeline_options"]["runner"],
            project=config["prod"]["pipeline_options"]["project"],
            job_name=config["prod"]["pipeline_options"]["job_name"],
            streaming=config["prod"]["pipeline_options"]["streaming"],
            temp_location=config["prod"]["pipeline_options"]["temp_location"],
            region=config["prod"]["pipeline_options"]["region"],
            max_num_workers=config["prod"]["pipeline_options"]["max_num_workers"]
        )
        table = config["prod"]["output_table"]
    else:
        raise ValueError("Inappropiate '--pipeline_status' args")

    with beam.Pipeline(options=beam_options) as p: 
        if known_args.pipeline_status=="dev":
            input = p | "Initialize" >> beam.Create([config["dev"]["dummy_input"]])
            data = input | "Read blobs" >> ReadAllFromText()

        elif known_args.pipeline_status=="prod":
            input = (
                p 
                    | "Read from pub/sub topic" >> ReadFromPubSub(
                        topic=config["prod"]["input_pubsub"],
                        with_attributes=True)
                    | "Take objectId" >> beam.ParDo(ReadPubSubObject(), config["prod"]["blob_path"]) 
            )
            
            data = input | "Read blobs" >> ReadAllFromText()

        transform = (
            data
                | "Convert JSON" >> beam.ParDo(ConvertDict())
        )

        transform | "Output" >> beam.io.WriteToBigQuery(
                    table=table, method="STREAMING_INSERTS", 
                    additional_bq_parameters=additional_bq_parameters,
                    insert_retry_strategy=RetryStrategy.RETRY_ON_TRANSIENT_ERROR,
                    ignore_unknown_columns=True)

if __name__=="__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()