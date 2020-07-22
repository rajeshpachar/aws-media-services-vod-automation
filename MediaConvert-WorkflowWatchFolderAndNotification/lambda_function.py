#!/usr/bin/env python

import glob
import json
import sys, traceback
import os
import uuid
import boto3
import datetime
import random
from urllib.parse import urlparse
import logging

from botocore.client import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

S3 = boto3.resource('s3')

jobsjson = {
  
    "OutputGroups": [
      {
        "CustomName": "abc",
        "Name": "File Group",
        "Outputs": [
          {
            "ContainerSettings": {
              "Container": "MP4",
              "Mp4Settings": {
                "CslgAtom": "INCLUDE",
                "CttsVersion": 0,
                "FreeSpaceBox": "EXCLUDE",
                "MoovPlacement": "PROGRESSIVE_DOWNLOAD"
              }
            },
            "VideoDescription": {
              "ScalingBehavior": "DEFAULT",
              "TimecodeInsertion": "DISABLED",
              "AntiAlias": "ENABLED",
              "Sharpness": 50,
              "CodecSettings": {
                "Codec": "H_264",
                "H264Settings": {
                  "InterlaceMode": "PROGRESSIVE",
                  "NumberReferenceFrames": 3,
                  "Syntax": "DEFAULT",
                  "Softness": 0,
                  "GopClosedCadence": 1,
                  "GopSize": 90,
                  "Slices": 1,
                  "GopBReference": "DISABLED",
                  "SlowPal": "DISABLED",
                  "SpatialAdaptiveQuantization": "ENABLED",
                  "TemporalAdaptiveQuantization": "ENABLED",
                  "FlickerAdaptiveQuantization": "DISABLED",
                  "EntropyEncoding": "CABAC",
                  "Bitrate": 1000000,
                  "FramerateControl": "INITIALIZE_FROM_SOURCE",
                  "RateControlMode": "CBR",
                  "CodecProfile": "MAIN",
                  "Telecine": "NONE",
                  "MinIInterval": 0,
                  "AdaptiveQuantization": "HIGH",
                  "CodecLevel": "AUTO",
                  "FieldEncoding": "PAFF",
                  "SceneChangeDetect": "ENABLED",
                  "QualityTuningLevel": "SINGLE_PASS",
                  "FramerateConversionAlgorithm": "DUPLICATE_DROP",
                  "UnregisteredSeiTimecode": "DISABLED",
                  "GopSizeUnits": "FRAMES",
                  "ParControl": "INITIALIZE_FROM_SOURCE",
                  "NumberBFramesBetweenReferenceFrames": 2,
                  "RepeatPps": "DISABLED",
                  "DynamicSubGop": "STATIC"
                }
              },
              "AfdSignaling": "NONE",
              "DropFrameTimecode": "ENABLED",
              "RespondToAfd": "NONE",
              "ColorMetadata": "INSERT"
            },
            "AudioDescriptions": [
              {
                "AudioTypeControl": "FOLLOW_INPUT",
                "AudioSourceName": "Audio Selector 1",
                "CodecSettings": {
                  "Codec": "AAC",
                  "AacSettings": {
                    "AudioDescriptionBroadcasterMix": "NORMAL",
                    "Bitrate": 96000,
                    "RateControlMode": "CBR",
                    "CodecProfile": "LC",
                    "CodingMode": "CODING_MODE_2_0",
                    "RawFormat": "NONE",
                    "SampleRate": 48000,
                    "Specification": "MPEG4"
                  }
                },
                "LanguageCodeControl": "FOLLOW_INPUT"
              }
            ],
            "NameModifier": "14_VikasSingh_1595319449"
          }
        ],
        "OutputGroupSettings": {
          "Type": "FILE_GROUP_SETTINGS",
          "FileGroupSettings": {
            "Destination": ""
          }
        }
      }
    ],
    "AdAvailOffset": 0,
    "Inputs": [
      {
        "AudioSelectors": {
          "Audio Selector 1": {
            "Offset": 0,
            "DefaultSelection": "DEFAULT",
            "ProgramSelection": 1
          }
        },
        "VideoSelector": {
          "ColorSpace": "FOLLOW",
          "Rotate": "AUTO",
          "AlphaBehavior": "DISCARD"
        },
        "FilterEnable": "AUTO",
        "PsiControl": "USE_PSI",
        "FilterStrength": 0,
        "DeblockFilter": "DISABLED",
        "DenoiseFilter": "DISABLED",
        "TimecodeSource": "ZEROBASED",
        "FileInput": "s3://mrappdrreddy-userfiles-mobilehub-1633072934/public/14_VikasSingh_1595319449.mp4"
      }
    ]
}

# s3://mrappdrreddy-userfiles-mobilehub-1633072934/public/temp/14_VikasSingh_1595319449_auto.mp4
def lambda_handler(event, context):
    '''
    Watchfolder handler - this lambda is triggered when video objects are uploaded to the 
    SourceS3Bucket/inputs folder.

    It will look for two sets of file inputs:
        SourceS3Bucket/inputs/SourceS3Key:
            the input video to be converted
        
        SourceS3Bucket/jobs/*.json:
            job settings for MediaConvert jobs to be run against the input video. If 
            there are no settings files in the jobs folder, then the Default job will be run 
            from the job.json file in lambda environment. 
    
    Ouput paths stored in outputGroup['OutputGroupSettings']['DashIsoGroupSettings']['Destination']
    are constructed from the name of the job settings files as follows:
        
        s3://<MediaBucket>/<basename(job settings filename)>/<basename(input)>/<Destination value from job settings file>

    '''

    assetID = str(uuid.uuid4())
    print(event)
    sourceS3Bucket = event['Records'][0]['s3']['bucket']['name']
    sourceS3Key = event['Records'][0]['s3']['object']['key']
    sourceS3 = 's3://'+ sourceS3Bucket + '/' + sourceS3Key
    destinationS3 =  sourceS3Bucket
    # os.environ['DestinationBucket']
    mediaConvertRole = "arn:aws:iam::983637169828:role/SimpleVOD-MediaConvertJobRole-1N84Z8MI59B7P"
    # os.environ['MediaConvertRole']
    application = "media-convert-h264"
    # os.environ['Application']
    region = "ap-south-1"
    # os.environ['AWS_DEFAULT_REGION']
    statusCode = 200
    jobs = []
    job = {}
    
                # "Destination": "s3://s3://mrappdrreddy-userfiles-mobilehub-1633072934/14_VikasSingh_1595319449/Default/public/temp/14_VikasSingh_1595319449_auto.mp4"

    # Use MediaConvert SDK UserMetadata to tag jobs with the assetID 
    # Events from MediaConvert will have the assetID in UserMedata
    jobMetadata = {}
    jobMetadata['assetID'] = assetID
    jobMetadata['application'] = application
    jobMetadata['input'] = sourceS3
    
    try:    
        
        # Build a list of jobs to run against the input.  Use the settings files in WatchFolder/jobs
        # if any exist.  Otherwise, use the default job.
        
        jobInput = {}
        # Iterates through all the objects in jobs folder of the WatchFolder bucket, doing the pagination for you. Each obj
        # contains a jobSettings JSON
        bucket = S3.Bucket(sourceS3Bucket)
        for obj in bucket.objects.filter(Prefix='jobs/'):
            if obj.key != "jobs/":
                jobInput = {}
                jobInput['filename'] = obj.key
                logger.info('jobInput: %s', jobInput['filename'])

                jobInput['settings'] = json.loads(obj.get()['Body'].read())
                logger.info(json.dumps(jobInput['settings'])) 
                
                jobs.append(jobInput)
        
        # Use Default job settings in the lambda zip file in the current working directory
        if not jobs:
            jobInput['filename'] = 'Default'
            logger.info('jobInput: %s', jobInput['filename'])

            jobInput['settings'] = jobsjson
            # json.load(json_data)
            logger.info(json.dumps(jobInput['settings']))

            jobs.append(jobInput)
                 
        # get the account-specific mediaconvert endpoint for this region
        mediaconvert_client = boto3.client('mediaconvert', region_name=region)
        endpoints = mediaconvert_client.describe_endpoints()

        # add the account-specific endpoint to the client session 
        client = boto3.client('mediaconvert', region_name=region, endpoint_url=endpoints['Endpoints'][0]['Url'], verify=False)
        
        for j in jobs:
            print(j)
            jobSettings = j['settings']
            jobFilename = j['filename']

            # Save the name of the settings file in the job userMetadata
            jobMetadata['settings'] = jobFilename

            # Update the job settings with the source video from the S3 event 
            jobSettings['Inputs'][0]['FileInput'] = sourceS3

            # Update the job settings with the destination paths for converted videos.  We want to replace the
            # destination bucket of the output paths in the job settings, but keep the rest of the
            # path
            destinationS3 = 's3://' + destinationS3 + '/public/output/'
            # +os.path.basename(sourceS3Key) 
            # \
            #     + os.path.splitext(os.path.basename(sourceS3Key))[0] + '/' \
            #     + os.path.splitext(os.path.basename(jobFilename))[0]                 

            print(destinationS3)

            for outputGroup in jobSettings['OutputGroups']:
                
                logger.info("outputGroup['OutputGroupSettings']['Type'] == %s", outputGroup['OutputGroupSettings']['Type']) 
                
                if outputGroup['OutputGroupSettings']['Type'] == 'FILE_GROUP_SETTINGS':
                    templateDestination = outputGroup['OutputGroupSettings']['FileGroupSettings']['Destination']
                    templateDestinationKey = urlparse(templateDestination).path
                    logger.info("templateDestinationKey == %s", templateDestinationKey)
                    outputGroup['OutputGroupSettings']['FileGroupSettings']['Destination'] = destinationS3
                    # +templateDestinationKey

                elif outputGroup['OutputGroupSettings']['Type'] == 'HLS_GROUP_SETTINGS':
                    templateDestination = outputGroup['OutputGroupSettings']['HlsGroupSettings']['Destination']
                    templateDestinationKey = urlparse(templateDestination).path
                    logger.info("templateDestinationKey == %s", templateDestinationKey)
                    outputGroup['OutputGroupSettings']['HlsGroupSettings']['Destination'] = destinationS3+templateDestinationKey
                
                elif outputGroup['OutputGroupSettings']['Type'] == 'DASH_ISO_GROUP_SETTINGS':
                    templateDestination = outputGroup['OutputGroupSettings']['DashIsoGroupSettings']['Destination']
                    templateDestinationKey = urlparse(templateDestination).path
                    logger.info("templateDestinationKey == %s", templateDestinationKey)
                    outputGroup['OutputGroupSettings']['DashIsoGroupSettings']['Destination'] = destinationS3+templateDestinationKey

                elif outputGroup['OutputGroupSettings']['Type'] == 'DASH_ISO_GROUP_SETTINGS':
                    templateDestination = outputGroup['OutputGroupSettings']['DashIsoGroupSettings']['Destination']
                    templateDestinationKey = urlparse(templateDestination).path
                    logger.info("templateDestinationKey == %s", templateDestinationKey)
                    outputGroup['OutputGroupSettings']['DashIsoGroupSettings']['Destination'] = destinationS3+templateDestinationKey
                
                elif outputGroup['OutputGroupSettings']['Type'] == 'MS_SMOOTH_GROUP_SETTINGS':
                    templateDestination = outputGroup['OutputGroupSettings']['MsSmoothGroupSettings']['Destination']
                    templateDestinationKey = urlparse(templateDestination).path
                    logger.info("templateDestinationKey == %s", templateDestinationKey)
                    outputGroup['OutputGroupSettings']['MsSmoothGroupSettings']['Destination'] = destinationS3+templateDestinationKey
                    
                elif outputGroup['OutputGroupSettings']['Type'] == 'CMAF_GROUP_SETTINGS':
                    templateDestination = outputGroup['OutputGroupSettings']['CmafGroupSettings']['Destination']
                    templateDestinationKey = urlparse(templateDestination).path
                    logger.info("templateDestinationKey == %s", templateDestinationKey)
                    outputGroup['OutputGroupSettings']['CmafGroupSettings']['Destination'] = destinationS3+templateDestinationKey
                else:
                    logger.error("Exception: Unknown Output Group Type %s", outputGroup['OutputGroupSettings']['Type'])
                    statusCode = 500
            
            logger.info(json.dumps(jobSettings))

            # Convert the video using AWS Elemental MediaConvert
            job = client.create_job(Role=mediaConvertRole, UserMetadata=jobMetadata, Settings=jobSettings)

    except Exception as e:
        logger.error('Exception: %s', e)
        statusCode = 500
        traceback.print_exc()
        raise

    finally:
        return {
            'statusCode': statusCode,
            'body': json.dumps(job, indent=4, sort_keys=True, default=str),
            'headers': {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'}
        }
