from basic_defs import cloud_storage, NAS
from boto3.session import Session
import boto3
from azure.storage.blob import ContainerClient
from azure.storage.blob import BlobServiceClient
from azure.storage.blob import BlobClient
from google.cloud import storage

import hashlib

import os
import sys

class AWS_S3(cloud_storage):
    def __init__(self):
        # TODO: Fill in the AWS access key ID
        self.access_key_id = ""
        # TODO: Fill in the AWS access secret key
        self.access_secret_key = ""
        # TODO: Fill in the bucket name
        self.bucket_name = ""
        
        
        self.session = Session(aws_access_key_id = self.access_key_id,
                                aws_secret_access_key = self.access_secret_key)
        self.s3 = self.session.resource('s3')
        self.bucket = self.s3.Bucket(self.bucket_name)

    # Implement the abstract functions from cloud_storage
    # Hints: Use the following APIs from boto3
    #     boto3.session.Session:
    #         https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html
    #     boto3.resources:
    #         https://boto3.amazonaws.com/v1/documentation/api/latest/guide/resources.html
    #     boto3.s3.Bucket:
    #         https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#bucket
    #     boto3.s3.Object:
    #         https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#object
    
    def list_blocks(self):
        blocks = []
        for bucket_object in self.bucket.objects.all():
            blocks.append(int(bucket_object.key))
        return blocks

    def read_block(self, offset):
        self.s3.Object(self.bucket_name,str(offset)).download_file(
            '/tmp/' + str(offset))
        with open('/tmp/' + str(offset), 'r') as f:
            return bytearray(f.read())

    def write_block(self, block, offset):
        #assert(len(block) == self.block_size)
        #file_name = self.create_temp_file(offset, block)
        #blocks = list_blocks(self):
        #if offset in block:
        #self.s3.Object(self.bucket_name, offset).delete()
        #self.bucket.upload_file(Filename=file_name, Key=file_name)
        self.bucket.put_object(Key=str(offset), Body=block)

    def delete_block(self, offset):
        self.s3.Object(self.bucket_name, str(offset)).delete()

class Azure_Blob_Storage(cloud_storage):
    def __init__(self):
        # TODO: Fill in the Azure key
        self.key = ""
        # TODO: Fill in the Azure connection string
        self.conn_str = ""
        # TODO: Fill in the account name
        self.account_name = ""
        # TODO: Fill in the container name
        self.container_name = ""
        
        self.container_client = ContainerClient.from_connection_string(self.conn_str, container_name=self.container_name)
    # Implement the abstract functions from cloud_storage
    # Hints: Use the following APIs from azure.storage.blob
    #    blob.BlobServiceClient:
    #        https://docs.microsoft.com/en-us/python/api/azure-storage-blob/azure.storage.blob.blobserviceclient?view=azure-python
    #    blob.ContainerClient:
    #        https://docs.microsoft.com/en-us/python/api/azure-storage-blob/azure.storage.blob.containerclient?view=azure-python
    #    blob.BlobClient:
    #        https://docs.microsoft.com/en-us/python/api/azure-storage-blob/azure.storage.blob.blobclient?view=azure-python

    def list_blocks(self):
        blobs = []
        blobs_list = self.container_client.list_blobs()
        for blob in blobs_list:
            blobs.append(int(blob.name))
        return blobs

    def read_block(self, offset):
        blob_client = BlobClient.from_connection_string(self.conn_str, container_name=self.container_name, blob_name=str(offset))
        data = blob_client.download_blob()
        return bytearray(data.readall())


    def write_block(self, block, offset):
        if int(offset) in self.list_blocks():
            self.delete_block(str(offset))
        blob_client = BlobClient.from_connection_string(self.conn_str, container_name=self.container_name, blob_name=str(offset))
        blob_client.upload_blob(block)

    def delete_block(self, offset):
        blob_client = BlobClient.from_connection_string(self.conn_str, container_name=self.container_name, blob_name=str(offset))
        if int(offset) in self.list_blocks():
            blob_client.delete_blob()


class Google_Cloud_Storage(cloud_storage):
    def __init__(self):
        # Google Cloud Storage is authenticated with a **Service Account**
        # TODO: Download and place the Credential JSON file
        self.credential_file = ""
        # TODO: Fill in the container name
        self.bucket_name = ""
        
        self.client = storage.Client.from_service_account_json(self.credential_file)
        self.bucket = self.client.get_bucket(self.bucket_name)
        
    # Implement the abstract functions from cloud_storage
    # Hints: Use the following APIs from google.cloud.storage
    #    storage.client.Client:
    #        https://googleapis.dev/python/storage/latest/client.html
    #    storage.bucket.Bucket:
    #        https://googleapis.dev/python/storage/latest/buckets.html
    #    storage.blob.Blob:
    #        https://googleapis.dev/python/storage/latest/blobs.html
    def list_blocks(self):
        return [int(y.name) for y in list(self.client.list_blobs(self.bucket))]

    def read_block(self, offset):
        blob = self.bucket.get_blob(str(offset))
        return bytearray(blob.download_as_bytes())#Return type bytes

    def write_block(self, block, offset):
        blob3 = self.bucket.blob(str(offset))
        if type(block) == type(bytearray()):
            blob3.upload_from_string(bytes(block))
        else :
            blob3.upload_from_string(block)
    def delete_block(self, offset):
        if int(offset) in self.list_blocks():
            self.bucket.delete_blob(str(offset))

class RAID_on_Cloud(NAS):
    def __init__(self):
        self.backends = [
                AWS_S3(),
                Azure_Blob_Storage(),
                Google_Cloud_Storage()
            ]
        self.opened = []

    # Implement the abstract functions from NAS
    
    
    #implement that hashing helper function
    def hash_function(self, filename, offset):
        return int(hashlib.md5(''.join([str(filename),str(offset)])).hexdigest(),16)
    def exist(self, offset):
        offset = int(offset)
        if offset in self.backends[offset%3].list_blocks():
            return True
        else:
            return False
    def allign(self, offset):
        offset = int(offset)
        return int(offset/cloud_storage.block_size),(offset%cloud_storage.block_size)
    
    
    def open(self, filename):
        #Create a file descriptor to represent the file. 
        #Because RAID-on-Cloud NAS does not store metadata in the cloud,
        #open(filename) does not distinguish whether the file has been previously created and written. 
        #open(filename) should always succeed and return a file descriptor. 
        #All files are opened as readable and writable.
        self.opened.append(filename)
        return filename
        #go to other code to look up how fd is implemented in local version
        
        #Let assume fd = {name of file}

    def read(self, fd, length, offset):
        #read(fd, length, offset): Read the data of the opened file descriptor, 
        #as the given length and offset, 
        #and return a byte array that contains the data. 
        #If the file does not exist in the cloud, 
        #or the offset has exceeded the end of the file, 
        #return a byte array with 0 byte.
        
        #print("start read:")
        
        data = bytearray()
        remain = length
        file_pointer = 0
        n_block, local_offset = self.allign(offset)
        file_pointer += n_block
        
        if fd not in self.opened:
            return data
        
        while remain >= cloud_storage.block_size:
            if self.exist(self.hash_function(fd,file_pointer)):
                block_to_read = self.backends[self.hash_function(fd,file_pointer)%3].read_block(self.hash_function(fd,file_pointer))
                data += block_to_read[local_offset:]
                remain -= len(block_to_read[local_offset:])
            else:
                return data
            local_offset = 0
            file_pointer += 1
        if remain > 0:
            if self.exist(self.hash_function(fd,file_pointer)):
                block_to_read = self.backends[self.hash_function(fd,file_pointer)%3].read_block(self.hash_function(fd,file_pointer))
                data += block_to_read[local_offset:local_offset + remain]
        return data


    def write(self, fd, data, offset):
        #Write the data store in a byte array into the opened file descriptor, 
        #at the given offset. No return value is needed. 
        #The function should always succeed. 
        #If the file is previously written and the newly written offset and length have overlapped with the original file size, 
        #the overlapped data will be overwritten. 
        #You must implement in-place updating to handle this corner case.
        
        #implementing based on read
        
        data_byte = bytearray(data)
        remain = len(data_byte) #remain bytes to write
        #print("remain is "+ str(remain))
        file_pointer = 0    #indes
        n_block, local_offset = self.allign(offset)
        #print("n_block, local_offset = " +str(n_block) +", " +str(local_offset) )
        
        if fd not in self.opened:
            return
        
        #handling corner cases between eof to offset, if there is any
        #print("write start running")
        while file_pointer * cloud_storage.block_size < offset:
            #print("file_pointer is " + str(file_pointer))
            if n_block == file_pointer:
                #print("check 1")
                break
            if not self.exist(self.hash_function(fd,file_pointer)):
                #print("check 2")
                self.backends[self.hash_function(fd,file_pointer)%3].write_block(bytearray([0]) * cloud_storage.block_size,self.hash_function(fd,file_pointer))
                #implementing back up here!
            file_pointer += 1
        block_to_write = bytearray()
        
        #print("check 3")
        #print("file_pointer = " + str(file_pointer))
        
        while remain > 0:
            if self.exist(self.hash_function(fd,file_pointer)):
                block_to_write = self.backends[self.hash_function(fd,file_pointer)%3].read_block(self.hash_function(fd,file_pointer))
            else:
                block_to_write = bytearray([0]) * cloud_storage.block_size
            if remain > len(block_to_write[local_offset:]):
                #print(list(block_to_write))
                #print(type(data_byte))
                
                block_to_write[local_offset:] = data_byte[:len(block_to_write[local_offset:])]
                del data_byte[:len(block_to_write[local_offset:])]
                remain -= len(block_to_write[local_offset:])
            else: #remain <= block
                #print("check 4")
                #print(list(data_byte))
                block_to_write[local_offset:local_offset+remain] = data_byte
                #print(list(block_to_write[local_offset:local_offset+remain]))
                #print(list(block_to_write[:100]))
                remain = 0
            self.backends[self.hash_function(fd,file_pointer)%3].write_block(block_to_write,self.hash_function(fd,file_pointer))
            file_pointer += 1
            local_offset = 0

    def close(self, fd):
        self.opened.remove(fd)
        return 0

    def delete(self, filename):

        file_pointer = 0
        
        while True:
            if self.exist(self.hash_function(filename,file_pointer)):
                self.backends[self.hash_function(filename,file_pointer)%3].delete_block(self.hash_function(filename,file_pointer))
                file_pointer += 1
            else:
                break
        

    def get_storage_sizes(self):
        return [len(b.list_blocks()) for b in self.backends]
    

