import { INodeExecutionData, INodeType, INodeTypeDescription, IExecuteFunctions, IHttpRequestMethods, IDataObject } from 'n8n-workflow';
import get from 'lodash/get';
import { parseString, Builder } from 'xml2js';
import { createHash } from 'crypto';
import axios, { AxiosRequestConfig, Method } from 'axios';
import { sign } from 'aws4';
import type { Readable } from 'stream';
import { parseRequestObject } from 'n8n-core'
import { paramCase, snakeCase } from 'change-case';
import { NodeOperationError } from 'n8n-workflow';
import { bucketFields, bucketOperations } from './BucketDescription';
import { folderFields, folderOperations } from './FolderDescription';
import { fileFields, fileOperations } from './FileDescription';
const UPLOAD_CHUNK_SIZE = 5120 * 1024;
function queryToString(params: IDataObject) {
	return Object.keys(params)
		.map((key) => key + '=' + (params[key] as string))
		.join('&');
}
async function makeRequest(options: any): Promise<any> {
	let axiosConfig: AxiosRequestConfig = {
			maxBodyLength: Infinity,
			maxContentLength: Infinity,
	};
	axiosConfig = Object.assign(axiosConfig, await parseRequestObject(options));
	const requestFn = async () => {
			try {
					return await axios(axiosConfig);
			} catch (error) {
					throw error;
			}
	};

	try {
			const response = await requestFn();
			let responseBody = response.data;
			responseBody = axiosConfig.responseType === 'arraybuffer' ? Buffer.alloc(0) : undefined;
			if (typeof responseBody === 'string' && responseBody.includes('<?xml version="1.0" encoding="UTF-8"?>')) {
					return new Promise((resolve, reject) => {
							parseString(responseBody, { explicitArray: false }, (err, data) => {
									if (err) {
											return reject(err);
									}
									resolve(data);
							});
					});
			}
			// Parse JSON response if applicable
			if (typeof responseBody === 'string') {
					return JSON.parse(responseBody);
			}
			// Return raw response if no special processing is needed
			return responseBody;
	} catch (error) {
			throw error;
	}
}
async function awsApiRequest(
  service: string,
  method: IHttpRequestMethods,
	path: string,
	body?: string | Buffer | any,
  query: IDataObject = {},
  headers?: object,
  key?: string,
  secret?: string,
  region?: string
): Promise<any> {
		const request = {
			qs: { service, path, query: {} },
			method,
			path: `${path}?${queryToString(query).replace(/\+/g, '%2B')}`,
			body,
			headers: {
					Host: `${service}.${region}.amazonaws.com`
			},
			encoding: null,
			resolveWithFullResponse: true,
			host: `${service}.${region}.amazonaws.com`,
			region
	};
	// Sign the request using aws4 library
	const signedRequest = sign(request, { accessKeyId: key, secretAccessKey: secret });
	const requestOptions: AxiosRequestConfig = {
			method: signedRequest.method as Method || 'GET',
			url: `https://${service}.${region}.amazonaws.com${path}`,
			data: signedRequest.body,
			headers: signedRequest.headers,
			responseType: 'arraybuffer'
	};
	const response = await makeRequest(requestOptions);
  return response
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
		credentials: [],
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
							// BUCKET
							...bucketOperations,
							...bucketFields,
							// FOLDER
							...folderOperations,
							...folderFields,
							// UPLOAD
							...fileOperations,
							...fileFields,
		],
	};
	async execute(this: IExecuteFunctions): Promise<INodeExecutionData[][]> {
				const items = this.getInputData();
				const returnData: INodeExecutionData[] = [];
				// const qs: IDataObject = {};
				let responseData;
				const resource = this.getNodeParameter('resource', 0);
				const operation = this.getNodeParameter('operation', 0);
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
				for (let i = 0; i < items.length; i++) {
					let headers: IDataObject = {};
					let qs: QueryString = {};
					try {
						if (resource === 'bucket') {
							if (operation === 'create') {
								const name: string = this.getNodeParameter('name', i) as string;
								const additionalFields: any = this.getNodeParameter('additionalFields', i);
								if (additionalFields.acl) {
									headers['x-amz-acl'] = paramCase(
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
									const builder = new Builder();
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
									headers['x-amz-storage-class'] =
										snakeCase(additionalFields.storageClass as string,)
										.toUpperCase();
								}
								const region = responseData.LocationConstraint._ as string;
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
									region as string,
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
										region as string,
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
									for (const childObject of responseData) {
										//@ts-ignore
										(body.Delete.Object as IDataObject[]).push({
											Key: childObject.Key as string,
										});
									}
									const builder = new Builder();
									const data = builder.buildObject(body);
									headers['Content-MD5'] = createHash('md5')
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
										region as string,
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
								const region = responseData.LocationConstraint._ as string;
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
										region as string,
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
										region as string,
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
									headers['x-amz-storage-class'] =
										snakeCase(additionalFields.storageClass as string)
										.toUpperCase();
								}
								if (additionalFields.acl) {
									headers['x-amz-acl'] = paramCase(additionalFields.acl as string);
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
								const region = responseData.LocationConstraint._ as string;
								responseData = await awsApiRequestREST(
									servicePath,
									'PUT',
									'',
									destination,
									qs,
									headers,
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
									throw new NodeOperationError(
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
									region as string,
								);
								let mimeType: string | undefined;
								if (response.headers['content-type']) {
									mimeType = response.headers['content-type'];
								}

								const newItem: INodeExecutionData = {
									json: items[i].json,
									binary: {},
								};

								if (items[i].binary !== undefined && newItem.binary) {
									Object.assign(newItem.binary, items[i].binary);
								}

								items[i] = newItem;

								const dataPropertyNameDownload = this.getNodeParameter('binaryPropertyName', i);

								const data = Buffer.from(response.body as string, 'utf8');

								items[i].binary![dataPropertyNameDownload] = await this.helpers.prepareBinaryData(
									data as unknown as Buffer,
									fileName,
									mimeType,
								);
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
								const region = responseData.LocationConstraint._ as string;
								responseData = await awsApiRequestREST(
									servicePath,
									'DELETE',
									`${basePath}/${fileKey}`,
									'',
									qs,
									{},
									accessKeyId,
									secretAccessKey,
									region as string,
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
								const region = responseData.LocationConstraint._ as string;
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
										region as string,
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
										region as string,
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
								const multipartHeaders: IDataObject = {};
								const neededHeaders: IDataObject = {};
								if (additionalFields.requesterPays) {
									neededHeaders['x-amz-request-payer'] = 'requester';
								}
								if (additionalFields.parentFolderKey) {
									path = `${basePath}/${additionalFields.parentFolderKey}/${fileName}`;
								}
								if (additionalFields.storageClass) {
									multipartHeaders['x-amz-storage-class'] =
										snakeCase(additionalFields.storageClass as string)
										.toUpperCase();
								}
								if (additionalFields.acl) {
									multipartHeaders['x-amz-acl'] = paramCase(
										additionalFields.acl as string,
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
								const region = responseData.LocationConstraint._ as string;
								if (isBinaryData) {
									const binaryPropertyName = this.getNodeParameter('binaryPropertyName', i);
									const binaryPropertyData = this.helpers.assertBinaryData(i, binaryPropertyName);
									let uploadData: Buffer | Readable;
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
											region as string,
										);
										const uploadId = createMultiPartUpload.InitiateMultipartUploadResult.UploadId;
										let part = 1;
										for await (const chunk of uploadData) {
											const chunkBuffer = await this.helpers.binaryToBuffer(chunk);
											const listHeaders = {
												'Content-Length': chunk.length,
												'Content-MD5': createHash('MD5')
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
													region as string,
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
													throw new NodeOperationError(this.getNode(), err);
												}
												if (error.response?.status !== 308) throw error;
											}
										}
										const listParts = (await awsApiRequestREST(
											servicePath,
											'GET',
											`${path}?max-parts=${900}&part-number-marker=0&uploadId=${uploadId}`,
											'',
											qs,
											{ ...neededHeaders },
											accessKeyId,
											secretAccessKey,
											region as string,
										)) as {
											ListPartsResult: {
												Part:
													| Array<{
															ETag: string;
															PartNumber: number;
														}>
													| {
															ETag: string;
															PartNumber: number;
														};
											};
										};
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
										const builder = new Builder();
										const data = builder.buildObject(body);
										const completeUpload = (await awsApiRequestREST(
											servicePath,
											'POST',
											`${path}?uploadId=${uploadId}`,
											data,
											qs,
											{
												...neededHeaders,
												'Content-MD5': createHash('md5')
													.update(data)
													.digest('base64'),
												'Content-Type': 'application/xml',
											},
											accessKeyId,
											secretAccessKey,
											region as string,
										)) as {
											CompleteMultipartUploadResult: {
												Location: string;
												Bucket: string;
												Key: string;
												ETag: string;
											};
										};
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
										headers['Content-MD5'] = createHash('md5')
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
											region as string,
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
									headers['Content-MD5'] = createHash('md5')
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
										region as string,
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
	}
}
