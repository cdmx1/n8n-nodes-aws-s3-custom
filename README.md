# n8n-nodes-aws-s3-custom

[n8n](https://n8n.io/) is a [fair-code licensed](https://docs.n8n.io/reference/license/) workflow automation platform.

[Installation](#installation)  
[Operations](#operations)  
[Compatibility](#compatibility)  
[Usage](#usage)
[Resources](#resources)  
[Version history](#version-history) 

## Installation

Follow the [installation guide](https://docs.n8n.io/integrations/community-nodes/installation/) in the n8n community nodes documentation.

### Main Differences from the Inbuilt S3 Node

1. **AWS SDK for JavaScript v3**: This custom version utilizes the AWS SDK for JavaScript version 3. This is a major difference from the inbuilt S3 node which may use an older version of the SDK. AWS SDK for JavaScript v3 offers several advantages including modular architecture, improved performance, and better support for modern JavaScript features.

2. **Credential Input and Bindings**: Unlike the builtin S3 node which on credential store, this custom version allows for dynamic credential management. Users can supply their AWS credentials as input parameters/bindings, allowing credentials to be retrieved dynamically from previous nodes or external secret stores. This enhances the usability and security of the integration, especially in scenarios where credentials need to be rotated frequently or sourced from external systems.

## Operations Supported
* Download File: Allows downloading a file from an AWS S3 bucket.
* Upload File: Allows uploading a file to an AWS S3 bucket.
* Copy File: Copies a file from one location to another within the same bucket or across different buckets.
* Move File: Moves a file from one location to another within the same bucket or across different buckets.
* Create Folder: Creates a new folder within the specified bucket.


