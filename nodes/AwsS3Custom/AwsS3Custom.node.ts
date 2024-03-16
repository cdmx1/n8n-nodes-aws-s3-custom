import { INodeExecutionData, INodeType, INodeTypeDescription, IExecuteFunctions, IHttpRequestMethods, IDataObject } from 'n8n-workflow';
import get from 'lodash/get';
import { S3Client, GetObjectCommand, CopyObjectCommand, DeleteObjectCommand, HeadObjectCommand, PutObjectCommand,DeleteObjectsCommand, ListObjectsCommand, ListObjectsV2Command } from "@aws-sdk/client-s3";
import { parseString, Builder } from 'xml2js';
import { createHash } from 'crypto';
import { sign } from 'aws4';
import type { Readable } from 'stream';
import { paramCase, snakeCase } from 'change-case';
import { NodeOperationError } from 'n8n-workflow';
import { bucketFields, bucketOperations } from './BucketDescription';
import { folderFields, folderOperations } from './FolderDescription';
import { fileFields, fileOperations } from './FileDescription';
const axios = require('axios');
const UPLOAD_CHUNK_SIZE = 5120 * 1024;
async function awsGetFile(context: any, bucketName: string, key: string, accessKeyId: any, secretAccessKey: any, region: string) {
	const client = new S3Client({
		credentials: {
			accessKeyId,
			secretAccessKey,
		},
		region,
	});
	try {
		const command = new GetObjectCommand({
			Bucket: bucketName,
			Key: key,
		});

		const response = await client.send(command);
		const chunks = [];
		for await (const chunk of response.Body as any) {
			chunks.push(chunk);
		}
		const fileContent = Buffer.concat(chunks);
		return {
            fileContent,
            metadata: {
                ContentType: response.ContentType
            }
        };

	} catch (error) {
			throw new NodeOperationError(
				context.getNode(),
				error,
			);
	}
}
async function awsCreateFolder(context: any, bucketName: string, folderName: string, accessKeyId: any, secretAccessKey: any, region: string) {
	const client = new S3Client({
		credentials: {
			accessKeyId,
			secretAccessKey,
		},
		region,
	});
	const folderKey = `${bucketName}/${folderName}/`;

	try {
			// Check if the folder already exists
			const headCommand = new HeadObjectCommand({
					Bucket: bucketName,
					Key: folderKey
			});
			await client.send(headCommand);
			let error_msg = `Folder '${folderName}' already exists.`;
				throw new NodeOperationError(
					context.getNode(),
					error_msg,
				);
	} catch (error) {
			// If the folder doesn't exist, proceed to create it
			if (error.name !== 'NotFound') {
				throw new NodeOperationError(
					context.getNode(),
					error,
				);
			}
	}
	// Create an empty file to mimic the folder
	const putCommand = new PutObjectCommand({
			Bucket: bucketName,
			Key: folderKey,
			Body: '',
	});

	await client.send(putCommand);
	return {
		succees: true
	}
}
async function awsDeleteFolder(context: any, bucketName: string, folderName: string, accessKeyId: any, secretAccessKey: any, region: string) {
	const client = new S3Client({
			credentials: {
					accessKeyId,
					secretAccessKey,
			},
			region,
	});
	const folderKey = `${folderName}/`;

	try {
			// Check if the folder exists before attempting to delete
			const headCommand = new HeadObjectCommand({
					Bucket: bucketName,
					Key: folderKey
			});
			await client.send(headCommand);
	} catch (error) {
			// If the folder doesn't exist, throw an error
			if (error.name === 'NotFound') {
					throw new NodeOperationError(
							context.getNode(),
							`Folder '${folderName}' does not exist.`
					);
			} else {
					// If any other error occurs, re-throw it
					throw new NodeOperationError(
							context.getNode(),
							error
					);
			}
	}

	// List objects within the folder to delete them
	const listObjectsCommand = new ListObjectsCommand({
			Bucket: bucketName,
			Prefix: folderKey
	});
	const { Contents } = await client.send(listObjectsCommand) as any;

	// Delete each object within the folder
	const deleteCommands = Contents.map((obj: { Key: any; }) => ({
			Bucket: bucketName,
			Key: obj.Key
	}));

	if (deleteCommands.length > 0) {
			const deleteObjectsCommand = new DeleteObjectsCommand({
					Bucket: bucketName,
					Delete: {
							Objects: deleteCommands
					}
			});
			await client.send(deleteObjectsCommand);
	}

	// Delete the folder itself
	const deleteFolderCommand = new DeleteObjectCommand({
			Bucket: bucketName,
			Key: folderKey
	});
	await client.send(deleteFolderCommand);

	return {
			success: true
	};
}
async function awsGetAll(context: any, bucketName: string, accessKeyId: any, secretAccessKey: any, region: string, options: { returnAll?: boolean, limit?: number } = {}) {
	const { returnAll = false, limit = 50 } = options;

	const client = new S3Client({
			credentials: {
					accessKeyId,
					secretAccessKey,
			},
			region,
	});

	const allItems = [];

	let continuationToken: string | undefined = undefined;
	let fetchedItems = 0;

	do {
			try {
					const listObjectsCommand: ListObjectsV2Command = new ListObjectsV2Command({
							Bucket: bucketName,
							ContinuationToken: continuationToken ?? undefined
					});

					const { Contents, NextContinuationToken } = await client.send(listObjectsCommand);

					allItems.push(...Contents as any[]);
					fetchedItems += (Contents as any[]).length;

					continuationToken = NextContinuationToken ?? undefined;
			} catch (error) {
					throw new NodeOperationError(
							context.getNode(),
							error
					);
			}
	} while ((returnAll || fetchedItems < limit) && continuationToken);

	return allItems;
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
		path,
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
	const requestOptions = {
		method: signedRequest.method,
		url: `https://${service}.${region}.amazonaws.com${path}`,
		body: signedRequest.body,
		headers: signedRequest.headers,
		encoding: null,
		resolveWithFullResponse: true,
		responseType: 'arraybuffer'
	};
	const response = await axios(requestOptions);
	try {
		if (response.includes('<?xml version="1.0" encoding="UTF-8"?>')) {
			return await new Promise((resolve, reject) => {
				parseString(response as string, { explicitArray: false }, (err: any, data: unknown) => {
					if (err) {
						return reject(err);
					}
					resolve(data);
				});
			});
		}
		return response;
	} catch (error) {
		return response;
	}
}
async function copyFileInS3(context: any, sourceBucketName: any, sourceKey: any, destinationBucketName: any, destinationKey: any, accessKeyId: any, secretAccessKey: any, region: any) {
	const client = new S3Client({
		credentials: {
			accessKeyId,
			secretAccessKey,
		},
		region,
	});
	try {

		const copyCommand = new CopyObjectCommand({
			CopySource: `/${sourceBucketName}/${sourceKey}`,
			Bucket: destinationBucketName,
			Key: destinationKey,
		});
		await client.send(copyCommand);
		return {
			success: true,
			message: "File copied successfully",
			source: {
				bucket: sourceBucketName,
				key: sourceKey
			},
			destination: {
				bucket: destinationBucketName,
				key: destinationKey
			}
		};
	} catch (error) {
			throw new NodeOperationError(
				context.getNode(),
				error,
			)
	}
}
async function moveFileInS3(context: any, sourceBucketName: any, sourceKey: any, destinationBucketName: any, destinationKey: any, accessKeyId: any, secretAccessKey: any, region: any) {
	try {
		const client = new S3Client({
			credentials: {
				accessKeyId,
				secretAccessKey,
			},
			region,
		});
		const copyCommand = new CopyObjectCommand({
			CopySource: `/${sourceBucketName}/${sourceKey}`,
			Bucket: destinationBucketName,
			Key: destinationKey,
		});
		await client.send(copyCommand);
		const deleteCommand = new DeleteObjectCommand({
			Bucket: sourceBucketName,
			Key: sourceKey,
		});
		await client.send(deleteCommand);
		return {
			success: true,
			message: "File moved successfully",
			source: {
				bucket: sourceBucketName,
				key: sourceKey
			},
			destination: {
				bucket: destinationBucketName,
				key: destinationKey
			}
		};
	} catch (error) {
		throw new NodeOperationError(
			context.getNode(),
			error,
		)
	}
}
async function deleteFileInS3(context: any, bucketName: any, key: any, accessKeyId: any, secretAccessKey: any, region: any) {
	try {
		const client = new S3Client({
			credentials: {
				accessKeyId,
				secretAccessKey,
			},
			region,
		});
		const deleteCommand = new DeleteObjectCommand({
			Bucket: bucketName,
			Key: key,
		});
		await client.send(deleteCommand);
		return [{
			success: true,
			message: "File deleted successfully",
			details: {
				bucket: bucketName,
				key: key
			}
		}];
	} catch (error) {
		throw new NodeOperationError(
			context.getNode(),
			error,
		)
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
	return response;
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
		let responseData: any[] | IDataObject | null | undefined;
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
						// const basePath = bucketName.includes('.') ? `/${bucketName}` : '';
						const folderName = this.getNodeParameter('folderName', i) as string;
						const additionalFields = this.getNodeParameter('additionalFields', i);
						// let path = `${basePath}/${folderName}/`;
						// if (additionalFields.requesterPays) {
						// 	headers['x-amz-request-payer'] = 'requester';
						// }
						// if (additionalFields.parentFolderKey) {
						// 	path = `${basePath}/${additionalFields.parentFolderKey}/${folderName}/`;
						// }
						if (additionalFields.storageClass) {
							headers['x-amz-storage-class'] =
								snakeCase(additionalFields.storageClass as string,)
									.toUpperCase();
						}

						responseData = await awsCreateFolder(
							this,
							bucketName,
							folderName,
							accessKeyId,
							secretAccessKey,
							region
						);
						const executionData = this.helpers.constructExecutionMetaData(
							this.helpers.returnJsonArray(responseData),
							{ itemData: { item: i } },
						);
						returnData.push(...executionData);
					}
					if (operation === 'delete') {
						const bucketName = this.getNodeParameter('bucketName', i) as string;
						const folderKey = this.getNodeParameter('folderKey', i) as string;
						responseData = await awsDeleteFolder(this, bucketName, folderKey, accessKeyId, secretAccessKey, region)
						const executionData = this.helpers.constructExecutionMetaData(
							this.helpers.returnJsonArray(responseData),
							{ itemData: { item: i } },
						);
						returnData.push(...executionData);
					}
					if (operation === 'getAll') {
						const bucketName = this.getNodeParameter('bucketName', i) as string;
						const returnAll = this.getNodeParameter('returnAll', 0);
						const limit = this.getNodeParameter('limit', 0)
            const params = { returnAll: returnAll, limit: limit}
						const options = this.getNodeParameter('options', 0)
						responseData = await awsGetAll(this, bucketName, accessKeyId, secretAccessKey, region, params);
					if (Array.isArray(responseData)) {
							responseData = responseData.filter(
									e => e?.Key && e.Key.endsWith('/') && e.Size === 0 && e.Key !== options.folderKey
							);
							if (qs.limit && responseData.length > qs.limit) {
									responseData = responseData.slice(0, qs.limit);
							}
							const executionData = responseData.map(item => {
									return this.helpers.constructExecutionMetaData(
											this.helpers.returnJsonArray(responseData as any),
											{ itemData: { item: i } }
									);
							});

							returnData.push(...executionData as any);
					}
					}
				}
				if (resource === 'file') {
					if (operation === 'move') {
						const sourcePath = this.getNodeParameter('sourcePath', i) as string;
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
						const sourceParts = sourcePath.split('/');
						const destinationParts = destinationPath.split('/');
						const sourceKey = sourceParts.slice(2).join('/');
						const sourceBucketName = sourceParts[1];
						const destinationBucketName = destinationParts[1];
						const destinationKey = destinationParts.slice(2).join('/');
						const response = await moveFileInS3(
							this,
							sourceBucketName,
							sourceKey,
							destinationBucketName,
							destinationKey,
							accessKeyId,
							secretAccessKey,
							region
						)
						const executionData = this.helpers.constructExecutionMetaData(this.helpers.returnJsonArray(response), { itemData: { item: i } });
						returnData.push(...executionData);
					}
					if (operation === 'copy') {
						const sourcePath = this.getNodeParameter('sourcePath', i) as string;
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
						const sourceParts = sourcePath.split('/');
						const destinationParts = destinationPath.split('/');
						const sourceKey = sourceParts.slice(2).join('/');
						const sourceBucketName = sourceParts[1];
						const destinationBucketName = destinationParts[1];
						const destinationKey = destinationParts.slice(2).join('/');
						const response = await copyFileInS3(
							this,
							sourceBucketName,
							sourceKey,
							destinationBucketName,
							destinationKey,
							accessKeyId,
							secretAccessKey,
							region
						)
						const executionData = this.helpers.constructExecutionMetaData(this.helpers.returnJsonArray(response), { itemData: { item: i } });
						returnData.push(...executionData);
					}
					if (operation === 'download') {
						const bucketName = this.getNodeParameter('bucketName', i) as string;
						const fileKey = this.getNodeParameter('fileKey', i) as string;
						const fileName = fileKey.split('/')[fileKey.split('/').length - 1];
						if (fileKey.substring(fileKey.length - 1) === '/') {
							throw new NodeOperationError(
								this.getNode(),
								'Downloading a whole directory is not yet supported, please provide a file key',
							);
						}
						const response = await awsGetFile(this, bucketName, fileKey, accessKeyId, secretAccessKey, region);
						const newItem: INodeExecutionData = {
							json: items[i].json,
							binary: {},
						};

						if (items[i].binary !== undefined && newItem.binary) {
							Object.assign(newItem.binary, items[i].binary);
						}
						items[i] = newItem;
						const dataPropertyNameDownload = this.getNodeParameter('binaryPropertyName', i);
						const data = Buffer.from(response.fileContent as any, 'utf8');
						items[i].binary![dataPropertyNameDownload] = await this.helpers.prepareBinaryData(
							data as unknown as Buffer,
							fileName,
							response.metadata.ContentType,
						);
					}
					if (operation === 'delete') {
						const bucketName = this.getNodeParameter('bucketName', i) as string;
						const fileKey = this.getNodeParameter('fileKey', i);
						const options = this.getNodeParameter('options', i);
						if (options.versionId) {
							qs.versionId = options.versionId;
						}
						const responseData = await deleteFileInS3(this, bucketName, fileKey, accessKeyId, secretAccessKey, region);
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
						qs.delimiter = '/';
						qs['list-type'] = 2;

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
