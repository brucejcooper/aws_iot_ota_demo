#!/usr/bin/env python3
from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient
import logging
import time
import json
import uuid
import asyncio
import cbor
import math

logging.basicConfig(level=logging.INFO)



OTA_CBOR_CLIENTTOKEN_KEY       = "c"
OTA_CBOR_FILEID_KEY            = "f"
OTA_CBOR_BLOCKSIZE_KEY         = "l"
OTA_CBOR_BLOCKOFFSET_KEY       = "o"
OTA_CBOR_BLOCKBITMAP_KEY       = "b"
OTA_CBOR_STREAMDESCRIPTION_KEY = "d"
OTA_CBOR_STREAMFILES_KEY       = "r"
OTA_CBOR_FILESIZE_KEY          = "z"
OTA_CBOR_BLOCKID_KEY           = "i"
OTA_CBOR_BLOCKPAYLOAD_KEY      = "p"
OTA_CBOR_NUMBEROFBLOCKS_KEY    = "n"



def create_client_token():
    '''Creates a token that can be used to correlate a request/response pair'''
    return str(uuid.uuid4())


class OTAStream:
    ''' 
    Streams an AWS IoT Stream from the cloud to local, one block at a time.  I wasn't able to find good documentation online about this, and used the following source as instruction:

    * https://github.com/aws/amazon-freertos/blob/master/libraries/freertos_plus/aws/ota/src/mqtt/aws_iot_ota_mqtt.c
    * https://github.com/aws/amazon-freertos/blob/master/libraries/freertos_plus/aws/ota/src/mqtt/aws_iot_ota_cbor.c

    The OTA code makes use of CBOR for its message payload format, with very short (1 character) key names

    '''
    def __init__(self, mqtt_client, deviceId, streamname, file_id, file_size, blocksize=4096):
        self.mqtt_client = mqtt_client
        self.file_id = file_id
        self.next_block_id = 0
        self.file_size = file_size
        self.blocksize = blocksize
        self.num_blocks = math.ceil(self.file_size / blocksize)
        self.request_topic = "$aws/things/{}/streams/{}/get/cbor".format(deviceId, streamname)
        self.data_topic = "$aws/things/{}/streams/{}/data/cbor".format(deviceId, streamname)
        self.block_future = None

    def __enter__(self):
        self.mqtt_client.subscribe(self.data_topic, 0, self._stream_callback)

    def __exit__(self, type, value, traceback):
        logging.info("Unsubscribing from stream data")
        self.mqtt_client.unsubscribe(self.data_topic)


    async def request_block(self, block_id):
        client_token = create_client_token()
        
        # Only asks for one block at a time because if you ask for more they can come in out of order.  You could get em all at once,
        # but you'd have to be able to reassmeble (or write out of order blocks while working out what the signature is) yourself.
        # The AWS provided OTA client does provide for multiple blocks.
        req = {
            OTA_CBOR_CLIENTTOKEN_KEY: client_token,
            OTA_CBOR_FILEID_KEY:      self.file_id,
            OTA_CBOR_BLOCKSIZE_KEY:   self.blocksize,
            OTA_CBOR_BLOCKOFFSET_KEY: block_id,
            OTA_CBOR_NUMBEROFBLOCKS_KEY: 1 
        }
        binary_payload = bytearray(cbor.dumps(req))

        self.block_future = asyncio.get_event_loop().create_future()

        self.mqtt_client.publish(self.request_topic, binary_payload, 0)
        file_id, block_id, block_size, block = await self.block_future
        return file_id, block_id, block_size, block,

    def _stream_callback(self, client, userdata, msg):
        response = cbor.loads(msg.payload)
        
        file_id = response[OTA_CBOR_FILEID_KEY]
        block_id = response[OTA_CBOR_BLOCKID_KEY]
        block_size = response[OTA_CBOR_BLOCKSIZE_KEY]
        block = response[OTA_CBOR_BLOCKPAYLOAD_KEY]
        self.loop.call_soon_threadsafe(self.block_future.set_result, (file_id, block_id, block_size, block))


    async def stream_all_blocks(self):
        self.loop = asyncio.get_event_loop()
        timeout_secs = 5
        block_id_req = 0
        while block_id_req < self.num_blocks: 
            try:
                file_id, block_id, block_size, block = await asyncio.wait_for(self.request_block(block_id_req), timeout_secs)
                # TODO Here's where you'd write the block to flash, or whatever.
                logging.info("Got File %d Block %d of size %d", file_id, block_id, block_size)

                block_id_req += 1
            except asyncio.TimeoutError:
                logging.error("Timeout exception while awaiting block %s. Will try again", block_id_req)
                # TODO this will try again.  If there's some sort of repeated problem there should be some sort of bail out mechanism.


class RpcMessageClient:
    ''' 
    Does RPC in a MQTT.  When a message is sent, it adds a 'clientToken' field, which is then used to correlate responses.  Responses are expected on a topic that is the same as the request topic, but with either /accepted or /rejected on the end of it.

    The main job of this class is to make it use the Python asyncio await mechanisms to wait for the response before continuing.  There should only be
    one message in flight with this approach.
    '''
    def __init__(self, mqqt_client, deviceId):
        self.mqtt_client = mqqt_client
        self.deviceId = deviceId
        self.subscription_topic = "$aws/things/{}/jobs/#".format(self.deviceId)

    def __enter__(self):
        self.mqtt_client.subscribe(self.subscription_topic, 1, self._rpc_callback)

    def __exit__(self, type, value, traceback):
        logging.info("Unsubscribing from JOB RPC responses")
        self.mqtt_client.unsubscribe(self.subscription_topic)


    def _rpc_callback(self, client, userdata, msg):
        '''
        Called whenever a message comes in.  Uses the clientToken field in the message to lookup the asyncio future that represents the message that was
        sent.  If the response topic matches the 'accepted' topic prefix, then set the future to success.  If it is rejected, then set it as an excpeiton.
        If it came in some other topic, get really confused (but treat it as a rejection)
        '''
        body = json.loads(msg.payload)
        logging.info("Received msg on %s: %s", msg.topic, body)

        clientToken = body['clientToken']
        if clientToken != self.expected_response_token:
            logging.error("Received message with client token that did not match expectation.  Ignoring")
        else:        
            if msg.topic == '{}/accepted'.format(self.call_topic):
                self.calling_loop.call_soon_threadsafe(self.response_future.set_result, body)
            elif msg.topic == '{}/rejected'.format(self.call_topic):
                logging.error("Jobs call rejected: %s", body)
                self.calling_loop.call_soon_threadsafe(self.response_future.set_exception, Exception(body))
            else:
                logging.error("Response to RPC call on unexpected topic. Expected %s, got %s", self.call_topic, msg.topic)
                self.calling_loop.call_soon_threadsafe(self.response_future.set_exception, Exception(body))
            self.expected_response_token = None

    async def call(self, subtopic, body):
        self.call_topic = "$aws/things/{}/jobs/{}".format(self.deviceId, subtopic)
        self.expected_response_token = create_client_token()
        self.calling_loop = asyncio.get_event_loop()
        self.response_future = self.calling_loop.create_future()

        body['clientToken'] = self.expected_response_token
        self.mqtt_client.publish(self.call_topic, json.dumps(body), 0)
        response = await self.response_future
        return response





class IoTJobExecutor:
    ''' 
    Represents an interface that can be used to exeucte jobs for an OTA 
    The message format for Jobs is described at https://docs.aws.amazon.com/iot/latest/developerguide/jobs-devices.html
    '''
    def __init__(self, mqtt_client, deviceId):
        self.deviceId = deviceId
        self.mqtt_client = mqtt_client
        self.rpc = RpcMessageClient(mqtt_client, deviceId)

    async def poll_for_update(self):
        ''' Ask for the "Next Job". The resonse will have an 'execution' element if there is one.  If there is, execute it.'''
        with self.rpc:
            next_job = await self.rpc.call("$next/get", {})
            logging.info("Got %s", next_job)
            return 'execution' in next_job

    async def execute_next_job(self, timeout_mins=None):
        ''' Finds and executes the next IoT core job for the device.  Throws an exception if there is no job'''
        params = {}

        with self.rpc:
            if timeout_mins is not None:
                params["stepTimeoutInMinutes"] = timeout_mins
            # Start the "next job", setting it to "IN_PROGRESS" state, with a timeout
            job_response = await self.rpc.call("start-next", params)

            if not 'execution' in job_response:
                raise Exception("No Job to execute")
            job_execution = job_response['execution']

            try:
                job_id = job_execution['jobId']
                version = job_execution['versionNumber']
                job_document = job_execution['jobDocument']

                # This is AFR OTA specific
                firmware_file = job_document['afr_ota']['files'][0]
                fileid = firmware_file['fileid']
                sig = firmware_file['sig-sha256-ecdsa']
                file_size = firmware_file['filesize']

                streamname = job_document['afr_ota']['streamname']

                logging.info("Job %s Streaming %s/%d with sig %s. Size is %f", job_id, streamname, fileid, sig, file_size)
                s = OTAStream(self.mqtt_client, self.deviceId, streamname, fileid, file_size)
                with s:
                    await s.stream_all_blocks()

                # TODO compare supplied signature against the public key / firmware validator on the chip.  Explicitly reject the job if it does not match

                # Update the status to SUCCEEDED. This can also be executed mid job to supply status updates (e.g. percentage complete)
                job_response = await self.rpc.call("{}/update".format(job_id), {
                    "status": "SUCCEEDED",
                    "expectedVersion": version,
                })
            except:
                logging.exception("Error processing stream")
                job_response = await self.rpc.call("{}/update".format(job_id), {                
                    "status": "FAILED",
                    "statusDetails": {
                        "reason": "Put in your excuse here"
                    },
                    "expectedVersion": version,
                })
            logging.info("Final job response is %s", job_response)

        



# For certificate based connection
myMQTTClient = AWSIoTMQTTClient("myClientID")
# TODO make sure you replace the endpoint and credentials with your own values
# For TLS mutual authentication
myMQTTClient.configureEndpoint("a2mg298nir3kar-ats.iot.ap-southeast-2.amazonaws.com", 8883)
myMQTTClient.configureCredentials("ca.pem", "a4bbfeb268-private.pem.key", "a4bbfeb268-certificate.pem.crt")

myMQTTClient.configureOfflinePublishQueueing(-1)  # Infinite offline Publish queueing
myMQTTClient.configureDrainingFrequency(2)  # Draining: 2 Hz
myMQTTClient.configureConnectDisconnectTimeout(10)  # 10 sec
myMQTTClient.configureMQTTOperationTimeout(5)  # 5 sec


logging.info("Connecting")
myMQTTClient.connect()

job_exec = IoTJobExecutor(myMQTTClient, deviceId = "OTATest")
asyncio.get_event_loop().run_until_complete(job_exec.execute_next_job())

logging.info("Shutting down")
myMQTTClient.disconnect()
