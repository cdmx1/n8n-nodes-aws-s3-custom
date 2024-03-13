import { getExecuteFunctions } from 'n8n-core';
import { INodeExecutionData, INodeType, INodeTypeDescription } from 'n8n-workflow';
import get from 'lodash/get';
const crypto = require('crypto');
const axios = require('axios');
const change_case = require('change-case');
const xml2js = require('xml2js');
const n8n_workflow = require('n8n-workflow');
const UPLOAD_CHUNK_SIZE = 5120 * 1024;
async function awsApiRequest(
  service: string,
  method: string,
  path: string,
  body: any,
  query: any,
  headers: any,
  key: string,
  secret: string,
  region: string
): Promise<any> {
	const currentDate: Date = new Date();
  const currentDateUTC: string = currentDate.toISOString().replace(/[:\-]|\.\d{3}/g, '');
  const bucket: string = path.split('/')[0];
  const signedHeaders: string = 'host;x-amz-content-sha256;x-amz-date';
  const credentialScope: string = `${currentDateUTC.substr(0, 8)}/${region}/${service}/aws4_request`;
  const algorithm: string = 'AWS4-HMAC-SHA256';

  const signingKey: Buffer = crypto.createHmac('sha256', Buffer.from(`AWS4${secret}`, 'utf8')).update(currentDateUTC.substr(0, 8), 'utf8').digest();
  const signature: Buffer = crypto.createHmac('sha256', signingKey).update(region, 'utf8').digest();
  const signatureFinal: Buffer = crypto.createHmac('sha256', signature).update(service, 'utf8').digest();
  const signatureFinalHex: string = signatureFinal.toString('hex');

  const authorizationHeader: string = `${algorithm} Credential=${key}/${credentialScope}, SignedHeaders=${signedHeaders}, Signature=${signatureFinalHex}`;
  const requestOptions: any = {
    method,
    url: `https://${bucket}.s3.${region}.amazonaws.com/${path}`,
    headers: {
      Host: `${bucket}.s3.${region}.amazonaws.com`,
      'X-Amz-Content-Sha256': 'UNSIGNED-PAYLOAD',
      'X-Amz-Date': currentDateUTC,
      Authorization: authorizationHeader,
      ...headers
    },
    resolveWithFullResponse: true
  };
  const response = await axios(requestOptions);
  try {
    if (response.data.includes('<?xml version="1.0" encoding="UTF-8"?>')) {
      return await new Promise((resolve, reject) => {
        xml2js.parseString(response.data, { explicitArray: false }, (err: any, data: any) => {
          if (err) {
            return reject(err);
          }
          resolve(data);
        });
      });
    } else {
      try {
        return JSON.parse(response.data);
      } catch (error) {
        return response.data;
      }
    }
  } catch (error) {
    return response.data;
  }
}

async function awsApiRequestREST(
	this: any,
	service: any,
	method: any,
	path: any,
	body: any,
	query = {},
	headers: any,
	key: string,
	secret: string,
	region: string
) {
	const response = await awsApiRequest(
		service,
		method,
		path,
		body,
		query,
		headers,
		key,
		secret,
		region,
	);
	return JSON.parse(response);
}
async function awsApiRequestRESTAllItems(
	propertyName: string,
	service: any,
	method: any,
	path: any,
	body: any,
	query = {},
	headers: any,
	key: string,
	secret: string,
	region: string,
) {
	const returnData = [];
	let responseData;
	do {
		interface Query {
			[key: string]: any;
		}
		const query: Query = {};
		responseData = await awsApiRequestREST(
			service,
			method,
			path,
			body,
			query,
			headers,
			key,
			secret,
			region
		);
		if (get(responseData, [propertyName.split('.')[0], 'NextContinuationToken'])) {
			query['continuation-token'] = get(responseData, [propertyName.split('.')[0], 'NextContinuationToken']);
	}
		if (get(responseData, propertyName)) {
			if (Array.isArray(get(responseData, propertyName))) {
					returnData.push(...get(responseData, propertyName));
			} else {
					returnData.push(get(responseData, propertyName));
			}
	}
		const limit = query.limit;
		if (limit && limit <= returnData.length) {
			return returnData;
		}
	} while (
    get(responseData, [propertyName.split('.')[0], 'IsTruncated']) !== undefined &&
    get(responseData, [propertyName.split('.')[0], 'IsTruncated']) !== 'false'
 );
	return returnData;
}
const regions = [
	{
		name: 'af-south-1',
		displayName: 'Africa',
		location: 'Cape Town',
	},
	{
		name: 'ap-east-1',
		displayName: 'Asia Pacific',
		location: 'Hong Kong',
	},
	{
		name: 'ap-south-1',
		displayName: 'Asia Pacific',
		location: 'Mumbai',
	},
	{
		name: 'ap-southeast-1',
		displayName: 'Asia Pacific',
		location: 'Singapore',
	},
	{
		name: 'ap-southeast-2',
		displayName: 'Asia Pacific',
		location: 'Sydney',
	},
	{
		name: 'ap-southeast-3',
		displayName: 'Asia Pacific',
		location: 'Jakarta',
	},
	{
		name: 'ap-northeast-1',
		displayName: 'Asia Pacific',
		location: 'Tokyo',
	},
	{
		name: 'ap-northeast-2',
		displayName: 'Asia Pacific',
		location: 'Seoul',
	},
	{
		name: 'ap-northeast-3',
		displayName: 'Asia Pacific',
		location: 'Osaka',
	},
	{
		name: 'ca-central-1',
		displayName: 'Canada',
		location: 'Central',
	},
	{
		name: 'eu-central-1',
		displayName: 'Europe',
		location: 'Frankfurt',
	},
	{
		name: 'eu-north-1',
		displayName: 'Europe',
		location: 'Stockholm',
	},
	{
		name: 'eu-south-1',
		displayName: 'Europe',
		location: 'Milan',
	},
	{
		name: 'eu-west-1',
		displayName: 'Europe',
		location: 'Ireland',
	},
	{
		name: 'eu-west-2',
		displayName: 'Europe',
		location: 'London',
	},
	{
		name: 'eu-west-3',
		displayName: 'Europe',
		location: 'Paris',
	},
	{
		name: 'me-south-1',
		displayName: 'Middle East',
		location: 'Bahrain',
	},
	{
		name: 'sa-east-1',
		displayName: 'South America',
		location: 'São Paulo',
	},
	{
		name: 'us-east-1',
		displayName: 'US East',
		location: 'N. Virginia',
	},
	{
		name: 'us-east-2',
		displayName: 'US East',
		location: 'Ohio',
	},
	{
		name: 'us-west-1',
		displayName: 'US West',
		location: 'N. California',
	},
	{
		name: 'us-west-2',
		displayName: 'US West',
		location: 'Oregon',
	},
];
interface ObjectWithKey {
	Key: any;
}
interface QueryString {
	prefix?: any;
	'fetch-owner'?: any;
	'list-type'?: any;
	limit?: any;
	versionId?: any;
	delimiter?: any;
}
export class AwsS3Custom implements INodeType {
	description: INodeTypeDescription = {
		displayName: 'AWS S3 Custom',
		name: 'awsS3Custom',
		icon: 'file:s3.svg',
		group: ['output'],
		version: 1,
		subtitle: '={{$parameter["operation"] + ": " + $parameter["resource"]}}',
		description: 'Sends data to AWS S3',
		defaults: {
			name: 'AWS S3 Custom',
		},
		inputs: ['main'],
		outputs: ['main'],
		credentials: [
			{
					name: 'aws',
					required: false,
			}
	  ],
		properties: [
			{
				displayName: 'Region',
				name: 'region',
				type: 'options',
				options: regions.map((r: any) => ({
					name: `${r.displayName} (${r.location}) - ${r.name}`,
					value: r.name,
				})),
				default: '',
			},
			{
				displayName: 'Access Key ID',
				name: 'accessKeyId',
				type: 'string',
				required: true,
				default: '',
			},
			{
				displayName: 'Secret Access Key',
				name: 'secretAccessKey',
				type: 'string',
				required: true,
				default: '',
				typeOptions: {
					password: true,
				},
			},
			{
				displayName: 'Resource',
				name: 'resource',
				type: 'options',
				noDataExpression: true,
				options: [
					{
						name: 'Bucket',
						value: 'bucket',
					},
					{
						name: 'File',
						value: 'file',
					},
					{
						name: 'Folder',
						value: 'folder',
					},
				],
				default: 'file',
			},
			{
				displayName: 'Operation',
				name: 'operation',
				type: 'options',
				noDataExpression: true,
				displayOptions: {
					show: {
						resource: ['bucket'],
					},
				},
				options: [
					{
						name: 'Create',
						value: 'create',
						description: 'Create a bucket',
						action: 'Create a bucket',
					},
					{
						name: 'Delete',
						value: 'delete',
						description: 'Delete a bucket',
						action: 'Delete a bucket',
					},
					{
						name: 'Get Many',
						value: 'getAll',
						description: 'Get many buckets',
						action: 'Get many buckets',
					},
					{
						name: 'Search',
						value: 'search',
						description: 'Search within a bucket',
						action: 'Search a bucket',
					},
				],
				default: 'create',
			},
			{
				displayName: 'Name',
				name: 'name',
				type: 'string',
				required: true,
				default: '',
				displayOptions: {
					show: {
						resource: ['bucket'],
						operation: ['create'],
					},
				},
				description:
					'A succinct description of the nature, symptoms, cause, or effect of the bucket',
			},
			{
				displayName: 'Additional Fields',
				name: 'additionalFields',
				type: 'collection',
				placeholder: 'Add Field',
				displayOptions: {
					show: {
						resource: ['bucket'],
						operation: ['create'],
					},
				},
				default: {},
				options: [
					{
						displayName: 'ACL',
						name: 'acl',
						type: 'options',
						options: [
							{
								name: 'Authenticated Read',
								value: 'authenticatedRead',
							},
							{
								name: 'Private',
								value: 'Private',
							},
							{
								name: 'Public Read',
								value: 'publicRead',
							},
							{
								name: 'Public Read Write',
								value: 'publicReadWrite',
							},
						],
						default: 'authenticatedRead',
						description: 'The canned ACL to apply to the bucket',
					},
					{
						displayName: 'Bucket Object Lock Enabled',
						name: 'bucketObjectLockEnabled',
						type: 'boolean',
						default: false,
						description: 'Whether you want S3 Object Lock to be enabled for the new bucket',
					},
					{
						displayName: 'Grant Full Control',
						name: 'grantFullControl',
						type: 'boolean',
						default: false,
						description:
							'Whether to allow grantee the read, write, read ACP, and write ACP permissions on the bucket',
					},
					{
						displayName: 'Grant Read',
						name: 'grantRead',
						type: 'boolean',
						default: false,
						description: 'Whether to allow grantee to list the objects in the bucket',
					},
					{
						displayName: 'Grant Read ACP',
						name: 'grantReadAcp',
						type: 'boolean',
						default: false,
						description: 'Whether to allow grantee to read the bucket ACL',
					},
					{
						displayName: 'Grant Write',
						name: 'grantWrite',
						type: 'boolean',
						default: false,
						description:
							'Whether to allow grantee to create, overwrite, and delete any object in the bucket',
					},
					{
						displayName: 'Grant Write ACP',
						name: 'grantWriteAcp',
						type: 'boolean',
						default: false,
						description: 'Whether to allow grantee to write the ACL for the applicable bucket',
					},
					{
						displayName: 'Region',
						name: 'region',
						type: 'string',
						default: '',
						description:
							'Region you want to create the bucket in, by default the buckets are created on the region defined on the credentials',
					},
				],
			},
			{
				displayName: 'Name',
				name: 'name',
				type: 'string',
				required: true,
				default: '',
				displayOptions: {
					show: {
						resource: ['bucket'],
						operation: ['delete'],
					},
				},
				description: 'Name of the AWS S3 bucket to delete',
			},
			{
				displayName: 'Return All',
				name: 'returnAll',
				type: 'boolean',
				displayOptions: {
					show: {
						operation: ['getAll'],
						resource: ['bucket'],
					},
				},
				default: false,
				description: 'Whether to return all results or only up to a given limit',
			},
			{
				displayName: 'Limit',
				name: 'limit',
				type: 'number',
				displayOptions: {
					show: {
						operation: ['getAll'],
						resource: ['bucket'],
						returnAll: [false],
					},
				},
				typeOptions: {
					minValue: 1,
				},
				default: 50,
				description: 'Max number of results to return',
			},
			{
				displayName: 'Bucket Name',
				name: 'bucketName',
				type: 'string',
				required: true,
				default: '',
				displayOptions: {
					show: {
						resource: ['bucket'],
						operation: ['search'],
					},
				},
			},
			{
				displayName: 'Return All',
				name: 'returnAll',
				type: 'boolean',
				displayOptions: {
					show: {
						operation: ['search'],
						resource: ['bucket'],
					},
				},
				default: false,
				description: 'Whether to return all results or only up to a given limit',
			},
			{
				displayName: 'Limit',
				name: 'limit',
				type: 'number',
				displayOptions: {
					show: {
						operation: ['search'],
						resource: ['bucket'],
						returnAll: [false],
					},
				},
				typeOptions: {
					minValue: 1,
				},
				default: 50,
				description: 'Max number of results to return',
			},
			{
				displayName: 'Additional Fields',
				name: 'additionalFields',
				type: 'collection',
				placeholder: 'Add Field',
				displayOptions: {
					show: {
						resource: ['bucket'],
						operation: ['search'],
					},
				},
				default: {},
				options: [
					{
						displayName: 'Delimiter',
						name: 'delimiter',
						type: 'string',
						default: '',
						description: 'A delimiter is a character you use to group keys',
					},
					{
						displayName: 'Encoding Type',
						name: 'encodingType',
						type: 'options',
						options: [
							{
								name: 'URL',
								value: 'url',
							},
						],
						default: 'url',
						description: 'Encoding type used by Amazon S3 to encode object keys in the response',
					},
					{
						displayName: 'Fetch Owner',
						name: 'fetchOwner',
						type: 'boolean',
						default: false,
					},
					{
						displayName: 'Prefix',
						name: 'prefix',
						type: 'string',
						default: '',
						description: 'Limits the response to keys that begin with the specified prefix',
					},
					{
						displayName: 'Requester Pays',
						name: 'requesterPays',
						type: 'boolean',
						default: false,
						description:
							'Whether the requester will pay for requests and data transfer. While Requester Pays is enabled, anonymous access to this bucket is disabled.',
					},
					{
						displayName: 'Start After',
						name: 'startAfter',
						type: 'string',
						default: '',
						description:
							'StartAfter is where you want Amazon S3 to start listing from. Amazon S3 starts listing after this specified key.',
					},
				],
			},
			{
				displayName: 'Operation',
				name: 'operation',
				type: 'options',
				noDataExpression: true,
				displayOptions: {
					show: {
						resource: ['folder'],
					},
				},
				options: [
					{
						name: 'Create',
						value: 'create',
						description: 'Create a folder',
						action: 'Create a folder',
					},
					{
						name: 'Delete',
						value: 'delete',
						description: 'Delete a folder',
						action: 'Delete a folder',
					},
					{
						name: 'Get Many',
						value: 'getAll',
						description: 'Get many folders',
						action: 'Get many folders',
					},
				],
				default: 'create',
			},
			{
				displayName: 'Bucket Name',
				name: 'bucketName',
				type: 'string',
				required: true,
				default: '',
				displayOptions: {
					show: {
						resource: ['folder'],
						operation: ['create'],
					},
				},
			},
			{
				displayName: 'Folder Name',
				name: 'folderName',
				type: 'string',
				required: true,
				default: '',
				displayOptions: {
					show: {
						resource: ['folder'],
						operation: ['create'],
					},
				},
			},
			{
				displayName: 'Additional Fields',
				name: 'additionalFields',
				type: 'collection',
				placeholder: 'Add Field',
				displayOptions: {
					show: {
						resource: ['folder'],
						operation: ['create'],
					},
				},
				default: {},
				options: [
					{
						displayName: 'Parent Folder Key',
						name: 'parentFolderKey',
						type: 'string',
						default: '',
						description: 'Parent folder you want to create the folder in',
					},
					{
						displayName: 'Requester Pays',
						name: 'requesterPays',
						type: 'boolean',
						default: false,
						description:
							'Whether the requester will pay for requests and data transfer. While Requester Pays is enabled, anonymous access to this bucket is disabled.',
					},
					{
						displayName: 'Storage Class',
						name: 'storageClass',
						type: 'options',
						options: [
							{
								name: 'Deep Archive',
								value: 'deepArchive',
							},
							{
								name: 'Glacier',
								value: 'glacier',
							},
							{
								name: 'Intelligent Tiering',
								value: 'intelligentTiering',
							},
							{
								name: 'One Zone IA',
								value: 'onezoneIA',
							},
							{
								name: 'Reduced Redundancy',
								value: 'RecudedRedundancy',
							},
							{
								name: 'Standard',
								value: 'standard',
							},
							{
								name: 'Standard IA',
								value: 'standardIA',
							},
						],
						default: 'standard',
						description: 'Amazon S3 storage classes',
					},
				],
			},
			{
				displayName: 'Bucket Name',
				name: 'bucketName',
				type: 'string',
				required: true,
				default: '',
				displayOptions: {
					show: {
						resource: ['folder'],
						operation: ['delete'],
					},
				},
			},
			{
				displayName: 'Folder Key',
				name: 'folderKey',
				type: 'string',
				required: true,
				default: '',
				displayOptions: {
					show: {
						resource: ['folder'],
						operation: ['delete'],
					},
				},
			},
			{
				displayName: 'Bucket Name',
				name: 'bucketName',
				type: 'string',
				required: true,
				default: '',
				displayOptions: {
					show: {
						resource: ['folder'],
						operation: ['getAll'],
					},
				},
			},
			{
				displayName: 'Return All',
				name: 'returnAll',
				type: 'boolean',
				displayOptions: {
					show: {
						operation: ['getAll'],
						resource: ['folder'],
					},
				},
				default: false,
				description: 'Whether to return all results or only up to a given limit',
			},
			{
				displayName: 'Limit',
				name: 'limit',
				type: 'number',
				displayOptions: {
					show: {
						operation: ['getAll'],
						resource: ['folder'],
						returnAll: [false],
					},
				},
				typeOptions: {
					minValue: 1,
				},
				default: 50,
				description: 'Max number of results to return',
			},
			{
				displayName: 'Options',
				name: 'options',
				type: 'collection',
				placeholder: 'Add Field',
				default: {},
				displayOptions: {
					show: {
						resource: ['folder'],
						operation: ['getAll'],
					},
				},
				options: [
					{
						displayName: 'Fetch Owner',
						name: 'fetchOwner',
						type: 'boolean',
						default: false,
						description:
							'Whether owner field is not present in listV2 by default, if you want to return owner field with each key in the result then set the fetch owner field to true',
					},
					{
						displayName: 'Folder Key',
						name: 'folderKey',
						type: 'string',
						default: '',
					},
				],
			},
			{
				displayName: 'Operation',
				name: 'operation',
				type: 'options',
				noDataExpression: true,
				displayOptions: {
					show: {
						resource: ['file'],
					},
				},
				options: [
					{
						name: 'Copy',
						value: 'copy',
						description: 'Copy a file',
						action: 'Copy a file',
					},
					{
						name: 'Delete',
						value: 'delete',
						description: 'Delete a file',
						action: 'Delete a file',
					},
					{
						name: 'Download',
						value: 'download',
						description: 'Download a file',
						action: 'Download a file',
					},
					{
						name: 'Get Many',
						value: 'getAll',
						description: 'Get many files',
						action: 'Get many files',
					},
					{
						name: 'Upload',
						value: 'upload',
						description: 'Upload a file',
						action: 'Upload a file',
					},
				],
				default: 'download',
			},
			{
				displayName: 'Source Path',
				name: 'sourcePath',
				type: 'string',
				required: true,
				default: '',
				placeholder: '/bucket/my-image.jpg',
				displayOptions: {
					show: {
						resource: ['file'],
						operation: ['copy'],
					},
				},
				description:
					'The name of the source bucket and key name of the source object, separated by a slash (/)',
			},
			{
				displayName: 'Destination Path',
				name: 'destinationPath',
				type: 'string',
				required: true,
				default: '',
				placeholder: '/bucket/my-second-image.jpg',
				displayOptions: {
					show: {
						resource: ['file'],
						operation: ['copy'],
					},
				},
				description:
					'The name of the destination bucket and key name of the destination object, separated by a slash (/)',
			},
			{
				displayName: 'Additional Fields',
				name: 'additionalFields',
				type: 'collection',
				placeholder: 'Add Field',
				displayOptions: {
					show: {
						resource: ['file'],
						operation: ['copy'],
					},
				},
				default: {},
				options: [
					{
						displayName: 'ACL',
						name: 'acl',
						type: 'options',
						options: [
							{
								name: 'Authenticated Read',
								value: 'authenticatedRead',
							},
							{
								name: 'AWS Exec Read',
								value: 'awsExecRead',
							},
							{
								name: 'Bucket Owner Full Control',
								value: 'bucketOwnerFullControl',
							},
							{
								name: 'Bucket Owner Read',
								value: 'bucketOwnerRead',
							},
							{
								name: 'Private',
								value: 'private',
							},
							{
								name: 'Public Read',
								value: 'publicRead',
							},
							{
								name: 'Public Read Write',
								value: 'publicReadWrite',
							},
						],
						default: 'private',
						description: 'The canned ACL to apply to the object',
					},
					{
						displayName: 'Grant Full Control',
						name: 'grantFullControl',
						type: 'boolean',
						default: false,
						description:
							'Whether to give the grantee READ, READ_ACP, and WRITE_ACP permissions on the object',
					},
					{
						displayName: 'Grant Read',
						name: 'grantRead',
						type: 'boolean',
						default: false,
						description: 'Whether to allow grantee to read the object data and its metadata',
					},
					{
						displayName: 'Grant Read ACP',
						name: 'grantReadAcp',
						type: 'boolean',
						default: false,
						description: 'Whether to allow grantee to read the object ACL',
					},
					{
						displayName: 'Grant Write ACP',
						name: 'grantWriteAcp',
						type: 'boolean',
						default: false,
						description: 'Whether to allow grantee to write the ACL for the applicable object',
					},
					{
						displayName: 'Lock Legal Hold',
						name: 'lockLegalHold',
						type: 'boolean',
						default: false,
						description: 'Whether a legal hold will be applied to this object',
					},
					{
						displayName: 'Lock Mode',
						name: 'lockMode',
						type: 'options',
						options: [
							{
								name: 'Governance',
								value: 'governance',
							},
							{
								name: 'Compliance',
								value: 'compliance',
							},
						],
						default: 'governance',
						description: 'The Object Lock mode that you want to apply to this object',
					},
					{
						displayName: 'Lock Retain Until Date',
						name: 'lockRetainUntilDate',
						type: 'dateTime',
						default: '',
						description: "The date and time when you want this object's Object Lock to expire",
					},
					{
						displayName: 'Metadata Directive',
						name: 'metadataDirective',
						type: 'options',
						options: [
							{
								name: 'Copy',
								value: 'copy',
							},
							{
								name: 'Replace',
								value: 'replace',
							},
						],
						default: 'copy',
						description:
							'Specifies whether the metadata is copied from the source object or replaced with metadata provided in the request',
					},
					{
						displayName: 'Requester Pays',
						name: 'requesterPays',
						type: 'boolean',
						default: false,
						description:
							'Whether the requester will pay for requests and data transfer. While Requester Pays is enabled, anonymous access to this bucket is disabled.',
					},
					{
						displayName: 'Server Side Encryption',
						name: 'serverSideEncryption',
						type: 'options',
						options: [
							{
								name: 'AES256',
								value: 'AES256',
							},
							{
								name: 'AWS:KMS',
								value: 'aws:kms',
							},
						],
						default: 'AES256',
						description:
							'The server-side encryption algorithm used when storing this object in Amazon S3',
					},
					{
						displayName: 'Server Side Encryption Context',
						name: 'serverSideEncryptionContext',
						type: 'string',
						default: '',
						description: 'Specifies the AWS KMS Encryption Context to use for object encryption',
					},
					{
						displayName: 'Server Side Encryption AWS KMS Key ID',
						name: 'encryptionAwsKmsKeyId',
						type: 'string',
						default: '',
						description: 'If x-amz-server-side-encryption is present and has the value of aws:kms',
					},
					{
						displayName: 'Server Side Encryption Customer Algorithm',
						name: 'serversideEncryptionCustomerAlgorithm',
						type: 'string',
						default: '',
						description:
							'Specifies the algorithm to use to when encrypting the object (for example, AES256)',
					},
					{
						displayName: 'Server Side Encryption Customer Key',
						name: 'serversideEncryptionCustomerKey',
						type: 'string',
						default: '',
						description:
							'Specifies the customer-provided encryption key for Amazon S3 to use in encrypting data',
					},
					{
						displayName: 'Server Side Encryption Customer Key MD5',
						name: 'serversideEncryptionCustomerKeyMD5',
						type: 'string',
						default: '',
						description:
							'Specifies the 128-bit MD5 digest of the encryption key according to RFC 1321',
					},
					{
						displayName: 'Storage Class',
						name: 'storageClass',
						type: 'options',
						options: [
							{
								name: 'Deep Archive',
								value: 'deepArchive',
							},
							{
								name: 'Glacier',
								value: 'glacier',
							},
							{
								name: 'Intelligent Tiering',
								value: 'intelligentTiering',
							},
							{
								name: 'One Zone IA',
								value: 'onezoneIA',
							},
							{
								name: 'Standard',
								value: 'standard',
							},
							{
								name: 'Standard IA',
								value: 'standardIA',
							},
						],
						default: 'standard',
						description: 'Amazon S3 storage classes',
					},
					{
						displayName: 'Tagging Directive',
						name: 'taggingDirective',
						type: 'options',
						options: [
							{
								name: 'Copy',
								value: 'copy',
							},
							{
								name: 'Replace',
								value: 'replace',
							},
						],
						default: 'copy',
						description:
							'Specifies whether the metadata is copied from the source object or replaced with metadata provided in the request',
					},
				],
			},
			{
				displayName: 'Bucket Name',
				name: 'bucketName',
				type: 'string',
				required: true,
				default: '',
				displayOptions: {
					show: {
						resource: ['file'],
						operation: ['upload'],
					},
				},
			},
			{
				displayName: 'File Name',
				name: 'fileName',
				type: 'string',
				default: '',
				placeholder: 'hello.txt',
				required: true,
				displayOptions: {
					show: {
						resource: ['file'],
						operation: ['upload'],
						binaryData: [false],
					},
				},
			},
			{
				displayName: 'File Name',
				name: 'fileName',
				type: 'string',
				default: '',
				displayOptions: {
					show: {
						resource: ['file'],
						operation: ['upload'],
						binaryData: [true],
					},
				},
				description: 'If not set the binary data filename will be used',
			},
			{
				displayName: 'Binary File',
				name: 'binaryData',
				type: 'boolean',
				default: true,
				displayOptions: {
					show: {
						operation: ['upload'],
						resource: ['file'],
					},
				},
				description: 'Whether the data to upload should be taken from binary field',
			},
			{
				displayName: 'File Content',
				name: 'fileContent',
				type: 'string',
				default: '',
				displayOptions: {
					show: {
						operation: ['upload'],
						resource: ['file'],
						binaryData: [false],
					},
				},
				placeholder: '',
				description: 'The text content of the file to upload',
			},
			{
				displayName: 'Input Binary Field',
				name: 'binaryPropertyName',
				type: 'string',
				default: 'data',
				required: true,
				displayOptions: {
					show: {
						operation: ['upload'],
						resource: ['file'],
						binaryData: [true],
					},
				},
				placeholder: '',
				hint: 'The name of the input binary field containing the file to be uploaded',
			},
			{
				displayName: 'Additional Fields',
				name: 'additionalFields',
				type: 'collection',
				placeholder: 'Add Field',
				displayOptions: {
					show: {
						resource: ['file'],
						operation: ['upload'],
					},
				},
				default: {},
				options: [
					{
						displayName: 'ACL',
						name: 'acl',
						type: 'options',
						options: [
							{
								name: 'Authenticated Read',
								value: 'authenticatedRead',
							},
							{
								name: 'AWS Exec Read',
								value: 'awsExecRead',
							},
							{
								name: 'Bucket Owner Full Control',
								value: 'bucketOwnerFullControl',
							},
							{
								name: 'Bucket Owner Read',
								value: 'bucketOwnerRead',
							},
							{
								name: 'Private',
								value: 'private',
							},
							{
								name: 'Public Read',
								value: 'publicRead',
							},
							{
								name: 'Public Read Write',
								value: 'publicReadWrite',
							},
						],
						default: 'private',
						description: 'The canned ACL to apply to the object',
					},
					{
						displayName: 'Grant Full Control',
						name: 'grantFullControl',
						type: 'boolean',
						default: false,
						description:
							'Whether to give the grantee READ, READ_ACP, and WRITE_ACP permissions on the object',
					},
					{
						displayName: 'Grant Read',
						name: 'grantRead',
						type: 'boolean',
						default: false,
						description: 'Whether to allow grantee to read the object data and its metadata',
					},
					{
						displayName: 'Grant Read ACP',
						name: 'grantReadAcp',
						type: 'boolean',
						default: false,
						description: 'Whether to allow grantee to read the object ACL',
					},
					{
						displayName: 'Grant Write ACP',
						name: 'grantWriteAcp',
						type: 'boolean',
						default: false,
						description: 'Whether to allow grantee to write the ACL for the applicable object',
					},
					{
						displayName: 'Lock Legal Hold',
						name: 'lockLegalHold',
						type: 'boolean',
						default: false,
						description: 'Whether a legal hold will be applied to this object',
					},
					{
						displayName: 'Lock Mode',
						name: 'lockMode',
						type: 'options',
						options: [
							{
								name: 'Governance',
								value: 'governance',
							},
							{
								name: 'Compliance',
								value: 'compliance',
							},
						],
						default: 'governance',
						description: 'The Object Lock mode that you want to apply to this object',
					},
					{
						displayName: 'Lock Retain Until Date',
						name: 'lockRetainUntilDate',
						type: 'dateTime',
						default: '',
						description: "The date and time when you want this object's Object Lock to expire",
					},
					{
						displayName: 'Parent Folder Key',
						name: 'parentFolderKey',
						type: 'string',
						default: '',
						description: 'Parent folder you want to create the file in',
					},
					{
						displayName: 'Requester Pays',
						name: 'requesterPays',
						type: 'boolean',
						default: false,
						description:
							'Whether the requester will pay for requests and data transfer. While Requester Pays is enabled, anonymous access to this bucket is disabled.',
					},
					{
						displayName: 'Server Side Encryption',
						name: 'serverSideEncryption',
						type: 'options',
						options: [
							{
								name: 'AES256',
								value: 'AES256',
							},
							{
								name: 'AWS:KMS',
								value: 'aws:kms',
							},
						],
						default: 'AES256',
						description:
							'The server-side encryption algorithm used when storing this object in Amazon S3',
					},
					{
						displayName: 'Server Side Encryption Context',
						name: 'serverSideEncryptionContext',
						type: 'string',
						default: '',
						description: 'Specifies the AWS KMS Encryption Context to use for object encryption',
					},
					{
						displayName: 'Server Side Encryption AWS KMS Key ID',
						name: 'encryptionAwsKmsKeyId',
						type: 'string',
						default: '',
						description: 'If x-amz-server-side-encryption is present and has the value of aws:kms',
					},
					{
						displayName: 'Server Side Encryption Customer Algorithm',
						name: 'serversideEncryptionCustomerAlgorithm',
						type: 'string',
						default: '',
						description:
							'Specifies the algorithm to use to when encrypting the object (for example, AES256)',
					},
					{
						displayName: 'Server Side Encryption Customer Key',
						name: 'serversideEncryptionCustomerKey',
						type: 'string',
						default: '',
						description:
							'Specifies the customer-provided encryption key for Amazon S3 to use in encrypting data',
					},
					{
						displayName: 'Server Side Encryption Customer Key MD5',
						name: 'serversideEncryptionCustomerKeyMD5',
						type: 'string',
						default: '',
						description:
							'Specifies the 128-bit MD5 digest of the encryption key according to RFC 1321',
					},
					{
						displayName: 'Storage Class',
						name: 'storageClass',
						type: 'options',
						options: [
							{
								name: 'Deep Archive',
								value: 'deepArchive',
							},
							{
								name: 'Glacier',
								value: 'glacier',
							},
							{
								name: 'Intelligent Tiering',
								value: 'intelligentTiering',
							},
							{
								name: 'One Zone IA',
								value: 'onezoneIA',
							},
							{
								name: 'Standard',
								value: 'standard',
							},
							{
								name: 'Standard IA',
								value: 'standardIA',
							},
						],
						default: 'standard',
						description: 'Amazon S3 storage classes',
					},
				],
			},
			{
				displayName: 'Tags',
				name: 'tagsUi',
				placeholder: 'Add Tag',
				type: 'fixedCollection',
				default: {},
				typeOptions: {
					multipleValues: true,
				},
				displayOptions: {
					show: {
						resource: ['file'],
						operation: ['upload'],
					},
				},
				options: [
					{
						name: 'tagsValues',
						displayName: 'Tag',
						values: [
							{
								displayName: 'Key',
								name: 'key',
								type: 'string',
								default: '',
							},
							{
								displayName: 'Value',
								name: 'value',
								type: 'string',
								default: '',
							},
						],
					},
				],
				description: 'Optional extra headers to add to the message (most headers are allowed)',
			},
			{
				displayName: 'Bucket Name',
				name: 'bucketName',
				type: 'string',
				required: true,
				default: '',
				displayOptions: {
					show: {
						resource: ['file'],
						operation: ['download'],
					},
				},
			},
			{
				displayName: 'File Key',
				name: 'fileKey',
				type: 'string',
				required: true,
				default: '',
				displayOptions: {
					show: {
						resource: ['file'],
						operation: ['download'],
					},
				},
			},
			{
				displayName: 'Put Output File in Field',
				name: 'binaryPropertyName',
				type: 'string',
				required: true,
				default: 'data',
				displayOptions: {
					show: {
						operation: ['download'],
						resource: ['file'],
					},
				},
				hint: 'The name of the output binary field to put the file in',
			},
			{
				displayName: 'Bucket Name',
				name: 'bucketName',
				type: 'string',
				required: true,
				default: '',
				displayOptions: {
					show: {
						resource: ['file'],
						operation: ['delete'],
					},
				},
			},
			{
				displayName: 'File Key',
				name: 'fileKey',
				type: 'string',
				required: true,
				default: '',
				displayOptions: {
					show: {
						resource: ['file'],
						operation: ['delete'],
					},
				},
			},
			{
				displayName: 'Options',
				name: 'options',
				type: 'collection',
				placeholder: 'Add Field',
				default: {},
				displayOptions: {
					show: {
						resource: ['file'],
						operation: ['delete'],
					},
				},
				options: [
					{
						displayName: 'Version ID',
						name: 'versionId',
						type: 'string',
						default: '',
					},
				],
			},
			{
				displayName: 'Bucket Name',
				name: 'bucketName',
				type: 'string',
				required: true,
				default: '',
				displayOptions: {
					show: {
						resource: ['file'],
						operation: ['getAll'],
					},
				},
			},
			{
				displayName: 'Return All',
				name: 'returnAll',
				type: 'boolean',
				displayOptions: {
					show: {
						operation: ['getAll'],
						resource: ['file'],
					},
				},
				default: false,
				description: 'Whether to return all results or only up to a given limit',
			},
			{
				displayName: 'Limit',
				name: 'limit',
				type: 'number',
				displayOptions: {
					show: {
						operation: ['getAll'],
						resource: ['file'],
						returnAll: [false],
					},
				},
				typeOptions: {
					minValue: 1,
				},
				default: 50,
				description: 'Max number of results to return',
			},
			{
				displayName: 'Options',
				name: 'options',
				type: 'collection',
				placeholder: 'Add Field',
				default: {},
				displayOptions: {
					show: {
						resource: ['file'],
						operation: ['getAll'],
					},
				},
				options: [
					{
						displayName: 'Fetch Owner',
						name: 'fetchOwner',
						type: 'boolean',
						default: false,
						description:
							'Whether owner field is not present in listV2 by default, if you want to return owner field with each key in the result then set the fetch owner field to true',
					},
					{
						displayName: 'Folder Key',
						name: 'folderKey',
						type: 'string',
						default: '',
					},
				],
			},
		],
	};
	async execute(this: ReturnType<typeof getExecuteFunctions>): Promise<INodeExecutionData[][]> {
			try {
				var _a;
				const items = this.getInputData();
				const returnData = [];
				let responseData;
				const region = this.getNodeParameter('region', 0) as string;
				const accessKeyId = this.getNodeParameter('accessKeyId', 0) as string;
				const secretAccessKey = this.getNodeParameter('secretAccessKey', 0) as string;
				const credentials = {
					region: region,
					accessKeyId: accessKeyId,
					secretAccessKey: secretAccessKey,
					temporaryCredentials: false,
					customEndpoints: false,
				};
				const resource = this.getNodeParameter('resource', 0);
				const operation = this.getNodeParameter('operation', 0);
				for (let i = 0; i < items.length; i++) {
					let headers: { [key: string]: any } = {};
					let qs: QueryString = {};
					try {
						if (resource === 'bucket') {
							if (operation === 'create') {
								const name: string = this.getNodeParameter('name', i) as string;
								const additionalFields: any = this.getNodeParameter('additionalFields', i);
								const headers: Record<string, string> = {};
								if (additionalFields.acl) {
									headers['x-amz-acl'] = change_case.paramCase(
										additionalFields.acl,
									) as string;
								}
								if (additionalFields.bucketObjectLockEnabled) {
									headers['x-amz-bucket-object-lock-enabled'] =
										additionalFields.bucketObjectLockEnabled as string;
								}
								if (additionalFields.grantFullControl) {
									headers['x-amz-grant-full-control'] = '';
								}
								let region: string = credentials.region as string;
								if (additionalFields.region) {
									region = additionalFields.region as string;
								}

								const body: Record<string, any> = {
									CreateBucketConfiguration: {
										$: {
											xmlns: 'http://s3.amazonaws.com/doc/2006-03-01/',
										},
									},
								};

								let data: string = '';
								if (region !== 'us-east-1') {
									body.CreateBucketConfiguration.LocationConstraint = [region];
									const builder = new xml2js.Builder();
									data = builder.buildObject(body);
								}
								responseData = await awsApiRequestREST(
									`${name}.s3`,
									'PUT',
									'',
									data,
									qs,
									headers,
									accessKeyId,
									secretAccessKey,
									region
								);

								const executionData = this.helpers.constructExecutionMetaData(
									this.helpers.returnJsonArray({ success: true }),
									{ itemData: { item: i } },
								);
								returnData.push(...executionData);
							}
							// Add similar blocks for other operations (delete, getAll, search)...
						}
						if (resource === 'folder') {
							if (operation === 'create') {
								const bucketName = this.getNodeParameter('bucketName', i) as string;
								const servicePath = bucketName.includes('.') ? 's3' : `${bucketName}.s3`;
								const basePath = bucketName.includes('.') ? `/${bucketName}` : '';
								const folderName = this.getNodeParameter('folderName', i);
								const additionalFields = this.getNodeParameter('additionalFields', i);
								let path = `${basePath}/${folderName}/`;
								if (additionalFields.requesterPays) {
									headers['x-amz-request-payer'] = 'requester';
								}
								if (additionalFields.parentFolderKey) {
									path = `${basePath}/${additionalFields.parentFolderKey}/${folderName}/`;
								}
								if (additionalFields.storageClass) {
									headers['x-amz-storage-class'] = change_case
										.snakeCase(additionalFields.storageClass)
										.toUpperCase();
								}
								const region = responseData.LocationConstraint._;
								responseData = await awsApiRequestREST(
									servicePath,
									'GET',
									basePath,
									'',
									{
										location: '',
									},
									{},
									accessKeyId,
									secretAccessKey,
									region
								);
								responseData = await awsApiRequestREST(
									servicePath,
									'PUT',
									path,
									'',
									qs,
									headers,
									accessKeyId,
									secretAccessKey,
									region
								);
								const executionData = this.helpers.constructExecutionMetaData(
									this.helpers.returnJsonArray({ success: true }),
									{ itemData: { item: i } },
								);
								returnData.push(...executionData);
							}
							if (operation === 'delete') {
								const bucketName = this.getNodeParameter('bucketName', i) as string;
								const servicePath = bucketName.includes('.') ? 's3' : `${bucketName}.s3`;
								const basePath = bucketName.includes('.') ? `/${bucketName}` : '';
								const folderKey = this.getNodeParameter('folderKey', i);
								const region: string = responseData.LocationConstraint._;
								responseData = await awsApiRequestREST(
									servicePath,
									'GET',
									basePath,
									'',
									{
										location: '',
									},
									{},
									accessKeyId,
									secretAccessKey,
									region
								);
								responseData = await awsApiRequestRESTAllItems(
									'ListBucketResult.Contents',
									servicePath,
									'GET',
									basePath,
									'',
									{ 'list-type': 2, prefix: folderKey },
									{},
									accessKeyId,
									secretAccessKey,
									region
								);
								if (responseData.length === 0) {
									responseData = await awsApiRequestREST(
										servicePath,
										'DELETE',
										`${basePath}/${folderKey}`,
										'',
										qs,
										{},
										accessKeyId,
										secretAccessKey,
										region
									);
									responseData = { deleted: [{ Key: folderKey }] };
								} else {
									const body = {
										Delete: {
											$: {
												xmlns: 'http://s3.amazonaws.com/doc/2006-03-01/',
											},
											Object: [],
										},
									};
									const objectsArray: ObjectWithKey[] = body.Delete.Object;
									for (const childObject of responseData) {
										objectsArray.push({
											Key: childObject.Key,
										});
									}
									const builder = new xml2js.Builder();
									const data = builder.buildObject(body);
									headers['Content-MD5'] = crypto
										.createHash('md5')
										.update(data)
										.digest('base64');
									headers['Content-Type'] = 'application/xml';
									responseData = await awsApiRequestREST(
										servicePath,
										'POST',
										`${basePath}/`,
										data,
										{ delete: '' },
										headers,
										accessKeyId,
										secretAccessKey,
										region
									);
									responseData = { deleted: responseData.DeleteResult.Deleted };
								}
								const executionData = this.helpers.constructExecutionMetaData(
									this.helpers.returnJsonArray(responseData),
									{ itemData: { item: i } },
								);
								returnData.push(...executionData);
							}
							if (operation === 'getAll') {
								const bucketName = this.getNodeParameter('bucketName', i) as string;
								const servicePath = bucketName.includes('.') ? 's3' : `${bucketName}.s3`;
								const basePath = bucketName.includes('.') ? `/${bucketName}` : '';
								const returnAll = this.getNodeParameter('returnAll', 0);
								const options = this.getNodeParameter('options', 0);
								if (options.folderKey) {
									qs.prefix = options.folderKey;
								}
								if (options.fetchOwner) {
									qs['fetch-owner'] = options.fetchOwner;
								}
								qs['list-type'] = 2;
								const region: string = responseData.LocationConstraint._;
								responseData = await awsApiRequestREST(
									servicePath,
									'GET',
									basePath,
									'',
									{
										location: '',
									},
									{},
									accessKeyId,
									secretAccessKey,
									region
								);
								if (returnAll) {
									responseData = await awsApiRequestRESTAllItems(
										'ListBucketResult.Contents',
										servicePath,
										'GET',
										basePath,
										'',
										qs,
										{},
										accessKeyId,
										secretAccessKey,
										region
									);
								} else {
									qs.limit = this.getNodeParameter('limit', 0);
									responseData = await awsApiRequestRESTAllItems(
										'ListBucketResult.Contents',
										servicePath,
										'GET',
										basePath,
										'',
										qs,
										{},
										accessKeyId,
										secretAccessKey,
										region
									);
								}
								if (Array.isArray(responseData)) {
									responseData = responseData.filter(
										(e) => e.Key.endsWith('/') && e.Size === '0' && e.Key !== options.folderKey,
									);
									if (qs.limit) {
										responseData = responseData.splice(0, qs.limit);
									}
									const executionData = this.helpers.constructExecutionMetaData(
										this.helpers.returnJsonArray(responseData),
										{ itemData: { item: i } },
									);
									returnData.push(...executionData);
								}
							}
						}
						if (resource === 'file') {
							if (operation === 'copy') {
								const sourcePath = this.getNodeParameter('sourcePath', i);
								const destinationPath = this.getNodeParameter('destinationPath', i) as string;
								const additionalFields = this.getNodeParameter('additionalFields', i);
								headers['x-amz-copy-source'] = sourcePath;
								if (additionalFields.requesterPays) {
									headers['x-amz-request-payer'] = 'requester';
								}
								if (additionalFields.storageClass) {
									headers['x-amz-storage-class'] = change_case
										.snakeCase(additionalFields.storageClass)
										.toUpperCase();
								}
								if (additionalFields.acl) {
									headers['x-amz-acl'] = change_case.paramCase(additionalFields.acl);
								}
								if (additionalFields.grantFullControl) {
									headers['x-amz-grant-full-control'] = '';
								}
								if (additionalFields.grantRead) {
									headers['x-amz-grant-read'] = '';
								}
								if (additionalFields.grantReadAcp) {
									headers['x-amz-grant-read-acp'] = '';
								}
								if (additionalFields.grantWriteAcp) {
									headers['x-amz-grant-write-acp'] = '';
								}
								if (additionalFields.lockLegalHold) {
									headers['x-amz-object-lock-legal-hold'] = additionalFields.lockLegalHold
										? 'ON'
										: 'OFF';
								}
								if (additionalFields.lockMode && typeof additionalFields.lockMode === 'string') {
									headers['x-amz-object-lock-mode'] = additionalFields.lockMode.toUpperCase();
								}
								if (additionalFields.lockRetainUntilDate) {
									headers['x-amz-object-lock-retain-until-date'] =
										additionalFields.lockRetainUntilDate;
								}
								if (additionalFields.serverSideEncryption) {
									headers['x-amz-server-side-encryption'] = additionalFields.serverSideEncryption;
								}
								if (additionalFields.encryptionAwsKmsKeyId) {
									headers['x-amz-server-side-encryption-aws-kms-key-id'] =
										additionalFields.encryptionAwsKmsKeyId;
								}
								if (additionalFields.serverSideEncryptionContext) {
									headers['x-amz-server-side-encryption-context'] =
										additionalFields.serverSideEncryptionContext;
								}
								if (additionalFields.serversideEncryptionCustomerAlgorithm) {
									headers['x-amz-server-side-encryption-customer-algorithm'] =
										additionalFields.serversideEncryptionCustomerAlgorithm;
								}
								if (additionalFields.serversideEncryptionCustomerKey) {
									headers['x-amz-server-side-encryption-customer-key'] =
										additionalFields.serversideEncryptionCustomerKey;
								}
								if (additionalFields.serversideEncryptionCustomerKeyMD5) {
									headers['x-amz-server-side-encryption-customer-key-MD5'] =
										additionalFields.serversideEncryptionCustomerKeyMD5;
								}
								if (
									additionalFields.taggingDirective &&
									typeof additionalFields.taggingDirective === 'string'
								) {
									headers['x-amz-tagging-directive'] =
										additionalFields.taggingDirective.toUpperCase();
								}
								if (
									additionalFields.metadataDirective &&
									typeof additionalFields.metadataDirective === 'string'
								) {
									headers['x-amz-metadata-directive'] =
										additionalFields.metadataDirective.toUpperCase();
								}
								const destinationParts = destinationPath.split('/');
								const bucketName = destinationParts[1];
								const servicePath = bucketName.includes('.') ? 's3' : `${bucketName}.s3`;
								const basePath = bucketName.includes('.') ? `/${bucketName}` : '';
								const destination = `${basePath}/${destinationParts
									.slice(2, destinationParts.length)
									.join('/')}`;
								const region = responseData.LocationConstraint._;
								responseData = await awsApiRequestREST(
									servicePath,
									'GET',
									basePath,
									'',
									{
										location: '',
									},
									{},
									accessKeyId,
									secretAccessKey,
									region
								);
								responseData = await awsApiRequestREST(
									servicePath,
									'PUT',
									destination,
									'',
									qs,
									headers,
									accessKeyId,
									secretAccessKey,
									region
								);
								const executionData = this.helpers.constructExecutionMetaData(
									this.helpers.returnJsonArray(responseData.CopyObjectResult),
									{ itemData: { item: i } },
								);
								returnData.push(...executionData);
							}
							if (operation === 'download') {
								const bucketName = this.getNodeParameter('bucketName', i) as string;
								let items: any[] = [];
								const servicePath = bucketName.includes('.') ? 's3' : `${bucketName}.s3`;
								const basePath = bucketName.includes('.') ? `/${bucketName}` : '';
								const fileKey = this.getNodeParameter('fileKey', i) as string;
								const fileName = fileKey.split('/')[fileKey.split('/').length - 1];
								if (fileKey.substring(fileKey.length - 1) === '/') {
									throw new n8n_workflow.NodeOperationError(
										this.getNode(),
										'Downloading a whole directory is not yet supported, please provide a file key',
									);
								}
								const response = await awsApiRequestREST(
									servicePath,
									'GET',
									`${basePath}/${fileKey}`,
									'',
									qs,
									{ encoding: null, resolveWithFullResponse: true },
									accessKeyId,
									secretAccessKey,
									region
								);
								let mimeType;
								if (response.headers['content-type']) {
									mimeType = response.headers['content-type'];
								}
								const newItem = {
									json: items[i].json,
									binary: {},
								};
								if (items[i].binary !== undefined && newItem.binary) {
									Object.assign(newItem.binary, items[i].binary);
								}
								items[i] = newItem;
								const dataPropertyNameDownload = this.getNodeParameter('binaryPropertyName', i);
								const data = Buffer.from(response.body, 'utf8');
								if (items[i]) {
									items[i].binary[dataPropertyNameDownload] = await this.helpers.prepareBinaryData(
										data,
										fileName,
										mimeType,
									);
								}
							}
							if (operation === 'delete') {
								const bucketName = this.getNodeParameter('bucketName', i) as string;
								const servicePath = bucketName.includes('.') ? 's3' : `${bucketName}.s3`;
								const basePath = bucketName.includes('.') ? `/${bucketName}` : '';
								const fileKey = this.getNodeParameter('fileKey', i);
								const options = this.getNodeParameter('options', i);
								if (options.versionId) {
									qs.versionId = options.versionId;
								}
								const region = responseData.LocationConstraint._;
								responseData = await awsApiRequestREST(
									servicePath,
									'GET',
									basePath,
									'',
									{
										location: '',
									},
									{},
									accessKeyId,
									secretAccessKey,
									region
								);
								responseData = await awsApiRequestREST(
									servicePath,
									'DELETE',
									`${basePath}/${fileKey}`,
									'',
									qs,
									{},
									accessKeyId,
									secretAccessKey,
									region
								);
								const executionData = this.helpers.constructExecutionMetaData(
									this.helpers.returnJsonArray({ success: true }),
									{ itemData: { item: i } },
								);
								returnData.push(...executionData);
							}
							if (operation === 'getAll') {
								const bucketName = this.getNodeParameter('bucketName', i) as string;
								const servicePath = bucketName.includes('.') ? 's3' : `${bucketName}.s3`;
								const basePath = bucketName.includes('.') ? `/${bucketName}` : '';
								const returnAll = this.getNodeParameter('returnAll', 0);
								const options = this.getNodeParameter('options', 0);
								if (options.folderKey) {
									qs.prefix = options.folderKey;
								}
								if (options.fetchOwner) {
									qs['fetch-owner'] = options.fetchOwner;
								}
								qs.delimiter = '/';
								qs['list-type'] = 2;
								const region: string = responseData.LocationConstraint._;
								responseData = await awsApiRequestREST(
									servicePath,
									'GET',
									basePath,
									'',
									{
										location: '',
									},
									{},
									accessKeyId,
									secretAccessKey,
									region
								);
								if (returnAll) {
									responseData = await awsApiRequestRESTAllItems(
										'ListBucketResult.Contents',
										servicePath,
										'GET',
										basePath,
										'',
										qs,
										{},
										accessKeyId,
										secretAccessKey,
										region
									);
								} else {
									qs.limit = this.getNodeParameter('limit', 0);
									responseData = await awsApiRequestRESTAllItems(
										'ListBucketResult.Contents',
										servicePath,
										'GET',
										basePath,
										'',
										qs,
										{},
										accessKeyId,
										secretAccessKey,
										region
									);
									responseData = responseData.splice(0, qs.limit);
								}
								if (Array.isArray(responseData)) {
									responseData = responseData.filter((e) => !e.Key.endsWith('/') && e.Size !== '0');
									if (qs.limit) {
										responseData = responseData.splice(0, qs.limit);
									}
									const executionData = this.helpers.constructExecutionMetaData(
										this.helpers.returnJsonArray(responseData),
										{ itemData: { item: i } },
									);
									returnData.push(...executionData);
								}
							}
							if (operation === 'upload') {
								const bucketName = this.getNodeParameter('bucketName', i) as string;
								const servicePath = bucketName.includes('.') ? 's3' : `${bucketName}.s3`;
								const basePath = bucketName.includes('.') ? `/${bucketName}` : '';
								const fileName = this.getNodeParameter('fileName', i);
								const isBinaryData = this.getNodeParameter('binaryData', i);
								const additionalFields = this.getNodeParameter('additionalFields', i);
								const tagsValues = (this.getNodeParameter('tagsUi', i) as { tagsValues?: any })
									?.tagsValues;
								let path = `${basePath}/${fileName}`;
								let body;
								const multipartHeaders: { [key: string]: any } = {};
								const neededHeaders: { [key: string]: any } = {};
								if (additionalFields.requesterPays) {
									neededHeaders['x-amz-request-payer'] = 'requester';
								}
								if (additionalFields.parentFolderKey) {
									path = `${basePath}/${additionalFields.parentFolderKey}/${fileName}`;
								}
								if (additionalFields.storageClass) {
									multipartHeaders['x-amz-storage-class'] = change_case
										.snakeCase(additionalFields.storageClass)
										.toUpperCase();
								}
								if (additionalFields.acl) {
									multipartHeaders['x-amz-acl'] = change_case.paramCase(
										additionalFields.acl,
									);
								}
								if (additionalFields.grantFullControl) {
									multipartHeaders['x-amz-grant-full-control'] = '';
								}
								if (additionalFields.grantRead) {
									multipartHeaders['x-amz-grant-read'] = '';
								}
								if (additionalFields.grantReadAcp) {
									multipartHeaders['x-amz-grant-read-acp'] = '';
								}
								if (additionalFields.grantWriteAcp) {
									multipartHeaders['x-amz-grant-write-acp'] = '';
								}
								if (additionalFields.lockLegalHold) {
									multipartHeaders['x-amz-object-lock-legal-hold'] = additionalFields.lockLegalHold
										? 'ON'
										: 'OFF';
								}
								if (additionalFields.lockMode && typeof additionalFields.lockMode === 'string') {
									multipartHeaders['x-amz-object-lock-mode'] =
										additionalFields.lockMode.toUpperCase();
								}
								if (additionalFields.lockRetainUntilDate) {
									multipartHeaders['x-amz-object-lock-retain-until-date'] =
										additionalFields.lockRetainUntilDate;
								}
								if (additionalFields.serverSideEncryption) {
									neededHeaders['x-amz-server-side-encryption'] =
										additionalFields.serverSideEncryption;
								}
								if (additionalFields.encryptionAwsKmsKeyId) {
									neededHeaders['x-amz-server-side-encryption-aws-kms-key-id'] =
										additionalFields.encryptionAwsKmsKeyId;
								}
								if (additionalFields.serverSideEncryptionContext) {
									neededHeaders['x-amz-server-side-encryption-context'] =
										additionalFields.serverSideEncryptionContext;
								}
								if (additionalFields.serversideEncryptionCustomerAlgorithm) {
									neededHeaders['x-amz-server-side-encryption-customer-algorithm'] =
										additionalFields.serversideEncryptionCustomerAlgorithm;
								}
								if (additionalFields.serversideEncryptionCustomerKey) {
									neededHeaders['x-amz-server-side-encryption-customer-key'] =
										additionalFields.serversideEncryptionCustomerKey;
								}
								if (additionalFields.serversideEncryptionCustomerKeyMD5) {
									neededHeaders['x-amz-server-side-encryption-customer-key-MD5'] =
										additionalFields.serversideEncryptionCustomerKeyMD5;
								}
								if (tagsValues) {
									const tags: string[] = [];
									tagsValues.forEach((o: any) => {
										tags.push(`${o.key}=${o.value}`);
									});
									multipartHeaders['x-amz-tagging'] = tags.join('&');
								}
								const region = responseData.LocationConstraint._;
								responseData = await awsApiRequestREST(
									servicePath,
									'GET',
									basePath,
									'',
									{
										location: '',
									},
									{},
									accessKeyId,
									secretAccessKey,
									region
								);
								if (isBinaryData) {
									const binaryPropertyName = this.getNodeParameter('binaryPropertyName', i);
									const binaryPropertyData = this.helpers.assertBinaryData(i, binaryPropertyName);
									let uploadData;
									multipartHeaders['Content-Type'] = binaryPropertyData.mimeType;
									if (binaryPropertyData.id) {
										uploadData = await this.helpers.getBinaryStream(
											binaryPropertyData.id,
											UPLOAD_CHUNK_SIZE,
										);
										const createMultiPartUpload: any = await awsApiRequestREST(
											servicePath,
											'POST',
											`${path}?uploads`,
											body,
											qs,
											{ ...neededHeaders, ...multipartHeaders },
											accessKeyId,
											secretAccessKey,
											region
										);
										const uploadId = createMultiPartUpload.InitiateMultipartUploadResult.UploadId;
										let part = 1;
										for await (const chunk of uploadData) {
											const chunkBuffer = await this.helpers.binaryToBuffer(chunk);
											const listHeaders = {
												'Content-Length': chunk.length,
												'Content-MD5': crypto
													.createHash('MD5')
													.update(chunkBuffer)
													.digest('base64'),
												...neededHeaders,
											};
											try {
												await awsApiRequestREST(
													servicePath,
													'PUT',
													`${path}?partNumber=${part}&uploadId=${uploadId}`,
													chunk,
													qs,
													listHeaders,
													accessKeyId,
													secretAccessKey,
													region
												);
												part++;
											} catch (error) {
												try {
													await awsApiRequestREST(
														servicePath,
														'DELETE',
														`${path}?uploadId=${uploadId}`,
														{},
														{},
														{},
														accessKeyId,
														secretAccessKey,
														region
													);
												} catch (err) {
													throw new n8n_workflow.NodeOperationError(this.getNode(), err);
												}
												if (
													((_a = error.response) === null || _a === void 0 ? void 0 : _a.status) !==
													308
												)
													throw error;
											}
										}
										const listParts = await awsApiRequestREST(
											servicePath,
											'GET',
											`${path}?max-parts=${900}&part-number-marker=0&uploadId=${uploadId}`,
											'',
											qs,
											{ ...neededHeaders },
											accessKeyId,
											secretAccessKey,
											region
										);
										if (!Array.isArray(listParts.ListPartsResult.Part)) {
											body = {
												CompleteMultipartUpload: {
													$: {
														xmlns: 'http://s3.amazonaws.com/doc/2006-03-01/',
													},
													Part: {
														ETag: listParts.ListPartsResult.Part.ETag,
														PartNumber: listParts.ListPartsResult.Part.PartNumber,
													},
												},
											};
										} else {
											body = {
												CompleteMultipartUpload: {
													$: {
														xmlns: 'http://s3.amazonaws.com/doc/2006-03-01/',
													},
													Part: listParts.ListPartsResult.Part.map((Part: any) => {
														return {
															ETag: Part.ETag,
															PartNumber: Part.PartNumber,
														};
													}),
												},
											};
										}
										const builder = new xml2js.Builder();
										const data = builder.buildObject(body);
										const completeUpload = await awsApiRequestREST(
											servicePath,
											'POST',
											`${path}?uploadId=${uploadId}`,
											data,
											qs,
											{
												...neededHeaders,
												'Content-MD5': crypto
													.createHash('md5')
													.update(data)
													.digest('base64'),
												'Content-Type': 'application/xml',
											},
											accessKeyId,
									secretAccessKey,
									region
										);
										responseData = {
											...completeUpload.CompleteMultipartUploadResult,
										};
									} else {
										const binaryDataBuffer = await this.helpers.getBinaryDataBuffer(
											i,
											binaryPropertyName,
										);
										body = binaryDataBuffer;
										headers = { ...neededHeaders, ...multipartHeaders };
										headers['Content-Type'] = binaryPropertyData.mimeType;
										headers['Content-MD5'] = crypto
											.createHash('md5')
											.update(body)
											.digest('base64');
										responseData = await awsApiRequestREST(
											servicePath,
											'PUT',
											path,
											body,
											qs,
											headers,
											accessKeyId,
											secretAccessKey,
											region
										);
									}
									const executionData = this.helpers.constructExecutionMetaData(
										this.helpers.returnJsonArray(
											responseData !== null && responseData !== void 0
												? responseData
												: { success: true },
										),
										{ itemData: { item: i } },
									);
									returnData.push(...executionData);
								} else {
									const fileContent = this.getNodeParameter('fileContent', i) as any;
									body = Buffer.from(fileContent, 'utf8');
									headers = { ...neededHeaders, ...multipartHeaders };
									headers['Content-Type'] = 'text/html';
									headers['Content-MD5'] = crypto
										.createHash('md5')
										.update(fileContent)
										.digest('base64');
									responseData = await awsApiRequestREST(
										servicePath,
										'PUT',
										path,
										body,
										qs,
										{},
										accessKeyId,
										secretAccessKey,
										region
									);
									const executionData = this.helpers.constructExecutionMetaData(
										this.helpers.returnJsonArray({ success: true }),
										{ itemData: { item: i } },
									);
									returnData.push(...executionData);
								}
							}
						}
					} catch (error) {
						if (this.continueOnFail()) {
							const executionData = this.helpers.constructExecutionMetaData(
								this.helpers.returnJsonArray({ error: error.message }),
								{ itemData: { item: i } },
							);
							returnData.push(...executionData);
							continue;
						}
						throw error;
					}
				}
				if (resource === 'file' && operation === 'download') {
					return [items];
				} else {
					return [returnData];
				}
			} catch (error) {
				throw error;
			};
	}
}
