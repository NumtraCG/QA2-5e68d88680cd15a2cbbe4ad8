import traceback
import sys
from operations import TopOperation
from operations import JoinOperation
from operations import AggregationOperation
from operations import FormulaOperation
from operations import FilterOperation
from connectors import DBFSConnector
from connectors import CosmosDBConnector
from datatransformations import TranformationsMainFlow
from automl import tpot_execution
from core import PipelineNotification
import json

try: 
	PipelineNotification.PipelineNotification().started_notification('5e68d88680cd15a2cbbe4ad9','5e68d53980cd15a2cbbe4a18','http://40.83.140.93:3200/pipeline/notify')
	QA2_DBFS = DBFSConnector.DBFSConnector.fetch([], {}, "5e68d88680cd15a2cbbe4ad9", spark, "{'url': '/Demo/MovieRatingsTrain.csv', 'file_type': 'Delimeted', 'dbfs_token': 'dapib8073bbfa952efa9d363b234ce06e2c6', 'dbfs_domain': 'westus.azuredatabricks.net', 'delimiter': ',', 'is_header': 'Use Header Line'}")

	PipelineNotification.PipelineNotification().completed_notification('5e68d88680cd15a2cbbe4ad9','5e68d53980cd15a2cbbe4a18','http://40.83.140.93:3200/pipeline/notify')
except Exception as ex: 
	PipelineNotification.PipelineNotification().failed_notification(ex,'5e68d88680cd15a2cbbe4ad9','5e68d53980cd15a2cbbe4a18','http://40.83.140.93:3200/pipeline/notify','http://40.83.140.93:3200/logs/getProductLogs')
	sys.exit(1)
try: 
	PipelineNotification.PipelineNotification().started_notification('5e68d88680cd15a2cbbe4ada','5e68d53980cd15a2cbbe4a18','http://40.83.140.93:3200/pipeline/notify')
	QA2_AutoFE = TranformationsMainFlow.TramformationMain.run(["5e68d88680cd15a2cbbe4ad9"],{"5e68d88680cd15a2cbbe4ad9": QA2_DBFS}, "5e68d88680cd15a2cbbe4ada", spark,json.dumps( {"FE": [{"transformationsData": {}, "feature": "UserId", "transformation": "", "type": "numeric", "replaceby": "mean", "selected": "True", "stats": {"count": "2587", "mean": "465.06", "stddev": "264.69", "min": "1", "max": "943", "missing": "0"}}, {"transformationsData": {}, "feature": "MovieId", "transformation": "", "type": "numeric", "replaceby": "mean", "selected": "True", "stats": {"count": "2587", "mean": "432.85", "stddev": "337.75", "min": "1", "max": "1656", "missing": "0"}}, {"transformationsData": {}, "feature": "Rating", "type": "real", "selected": "True", "replaceby": "mean", "stats": {"count": "2587", "mean": "3.52", "stddev": "1.15", "min": "1.0", "max": "5.0", "missing": "0"}, "transformation": ""}, {"transformationsData": {"feature_label": "Timestamp"}, "feature": "Timestamp", "type": "date", "selected": "True", "replaceby": "random", "stats": {"count": "", "mean": "", "stddev": "", "min": "", "max": "", "missing": "0"}, "transformation": "Extract Date"}, {"transformationsData": {}, "feature": "AvgRating", "type": "real", "selected": "True", "replaceby": "mean", "stats": {"count": "2587", "mean": "3.53", "stddev": "0.44", "min": "1.51", "max": "4.67", "missing": "0"}, "transformation": ""}, {"feature": "Timestamp_dayofmonth", "transformation": "", "transformationsData": {}, "type": "numeric", "generated": "True", "selected": "True", "stats": {"count": "2587", "mean": "16.05", "stddev": "9.06", "min": "1", "max": "31", "missing": "0"}}, {"feature": "Timestamp_month", "transformation": "", "transformationsData": {}, "type": "numeric", "generated": "True", "selected": "True", "stats": {"count": "2587", "mean": "7.0", "stddev": "4.34", "min": "1", "max": "12", "missing": "0"}}, {"feature": "Timestamp_year", "transformation": "", "transformationsData": {}, "type": "numeric", "generated": "True", "selected": "True", "stats": {"count": "2587", "mean": "1997.45", "stddev": "0.5", "min": "1997", "max": "1998", "missing": "0"}}]}))

	PipelineNotification.PipelineNotification().completed_notification('5e68d88680cd15a2cbbe4ada','5e68d53980cd15a2cbbe4a18','http://40.83.140.93:3200/pipeline/notify')
except Exception as ex: 
	PipelineNotification.PipelineNotification().failed_notification(ex,'5e68d88680cd15a2cbbe4ada','5e68d53980cd15a2cbbe4a18','http://40.83.140.93:3200/pipeline/notify','http://40.83.140.93:3200/logs/getProductLogs')
	sys.exit(1)
try: 
	PipelineNotification.PipelineNotification().started_notification('5e68d88680cd15a2cbbe4adb','5e68d53980cd15a2cbbe4a18','http://40.83.140.93:3200/pipeline/notify')
	QA2_AutoML = tpot_execution.Tpot_execution.run(["5e68d88680cd15a2cbbe4ada"],{"5e68d88680cd15a2cbbe4ada": QA2_AutoFE}, "5e68d88680cd15a2cbbe4adb", spark,json.dumps( {"model_type": "regression", "label": "AvgRating", "features": ["UserId", "MovieId", "Rating", "Timestamp", "Timestamp_dayofmonth", "Timestamp_month", "Timestamp_year"], "percentage": "10", "executionTime": "5", "sampling": "0", "sampling_value": "", "run_id": "7f886ea51d7a439eae29cca95a6c5c8e", "model_id": "5e69f60480cd15a2cbbe4d49", "ProjectName": "TestProject", "PipelineName": "QA2", "pipelineId": "5e68d88680cd15a2cbbe4ad8", "userid": "5e68d53980cd15a2cbbe4a18", "runid": "", "url_ResultView": "http://40.83.140.93:3200", "experiment_id": "551308251382540"}))

	PipelineNotification.PipelineNotification().completed_notification('5e68d88680cd15a2cbbe4adb','5e68d53980cd15a2cbbe4a18','http://40.83.140.93:3200/pipeline/notify')
except Exception as ex: 
	PipelineNotification.PipelineNotification().failed_notification(ex,'5e68d88680cd15a2cbbe4adb','5e68d53980cd15a2cbbe4a18','http://40.83.140.93:3200/pipeline/notify','http://40.83.140.93:3200/logs/getProductLogs')
	sys.exit(1)

