import {
	INodeExecutionData,
	INodeType,
	INodeTypeDescription,
	IExecuteFunctions,
	IDataObject,
} from 'n8n-workflow';
import { Readable } from 'stream';
import { paramCase, snakeCase } from 'change-case';
import { NodeOperationError } from 'n8n-workflow';
import { bucketFields, bucketOperations } from './BucketDescription';
import { folderFields, folderOperations } from './FolderDescription';
import { fileFields, fileOperations } from './FileDescription';
import {
	awsGetFile,
	copyFileInS3,
	moveFileInS3,
	awsCreateFolder,
	awsDeleteFolder,
	deleteFileInS3,
	awsGetAll,
	createS3Bucket,
	regions,
	uploadFileToS3,
} from './GenericFunctions';
// const UPLOAD_CHUNK_SIZE = 5120 * 1024;
// import get from 'lodash/get';
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
			try {
				if (resource === 'bucket') {
					if (operation === 'create') {
						const name: string = this.getNodeParameter('name', i) as string;
						const additionalFields: any = this.getNodeParameter('additionalFields', i);
						if (additionalFields.acl) {
							headers['x-amz-acl'] = paramCase(additionalFields.acl) as string;
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
						responseData = await createS3Bucket(
							this,
							name,
							accessKeyId,
							secretAccessKey,
							region,
							additionalFields,
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
							headers['x-amz-storage-class'] = snakeCase(
								additionalFields.storageClass as string,
							).toUpperCase();
						}

						responseData = await awsCreateFolder(
							this,
							bucketName,
							folderName,
							accessKeyId,
							secretAccessKey,
							region,
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
						responseData = await awsDeleteFolder(
							this,
							bucketName,
							folderKey,
							accessKeyId,
							secretAccessKey,
							region,
						);
						const executionData = this.helpers.constructExecutionMetaData(
							this.helpers.returnJsonArray(responseData),
							{ itemData: { item: i } },
						);
						returnData.push(...executionData);
					}
					if (operation === 'getAll') {
						const bucketName = this.getNodeParameter('bucketName', i) as string;
						const returnAll = this.getNodeParameter('returnAll', 0);
						const limit = this.getNodeParameter('limit', 0);
						const params = { returnAll: returnAll, limit: limit };
						const options = this.getNodeParameter('options', 0);
						responseData = await awsGetAll(
							this,
							bucketName,
							accessKeyId,
							secretAccessKey,
							region,
							params,
						);
						if (Array.isArray(responseData)) {
							responseData = responseData.filter(
								(e) => e?.Key && e.Key.endsWith('/') && e.Size === 0 && e.Key !== options.folderKey,
							);
							const executionData = responseData.map((item) => {
								return this.helpers.constructExecutionMetaData(
									this.helpers.returnJsonArray(responseData as any),
									{ itemData: { item: i } },
								);
							});

							returnData.push(...(executionData as any));
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
							headers['x-amz-storage-class'] = snakeCase(
								additionalFields.storageClass as string,
							).toUpperCase();
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
							headers['x-amz-object-lock-retain-until-date'] = additionalFields.lockRetainUntilDate;
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
							headers['x-amz-tagging-directive'] = additionalFields.taggingDirective.toUpperCase();
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
							region,
						);
						const executionData = this.helpers.constructExecutionMetaData(
							this.helpers.returnJsonArray(response),
							{ itemData: { item: i } },
						);
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
							headers['x-amz-storage-class'] = snakeCase(
								additionalFields.storageClass as string,
							).toUpperCase();
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
							headers['x-amz-object-lock-retain-until-date'] = additionalFields.lockRetainUntilDate;
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
							headers['x-amz-tagging-directive'] = additionalFields.taggingDirective.toUpperCase();
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
							region,
						);
						const executionData = this.helpers.constructExecutionMetaData(
							this.helpers.returnJsonArray(response),
							{ itemData: { item: i } },
						);
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
						const response = await awsGetFile(
							this,
							bucketName,
							fileKey,
							accessKeyId,
							secretAccessKey,
							region,
						);
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
						const responseData = await deleteFileInS3(
							this,
							bucketName,
							fileKey,
							accessKeyId,
							secretAccessKey,
							region,
						);
						const executionData = this.helpers.constructExecutionMetaData(
							this.helpers.returnJsonArray(responseData),
							{ itemData: { item: i } },
						);
						returnData.push(...executionData);
					}
					if (operation === 'upload') {
						const bucketName = this.getNodeParameter('bucketName', i) as string;
						const fileName = this.getNodeParameter('fileName', i) as string;
						// const isBinaryData = this.getNodeParameter('binaryData', i);
						const additionalFields = this.getNodeParameter('additionalFields', i);
						const tagsValues = (this.getNodeParameter('tagsUi', i) as { tagsValues?: any })
							?.tagsValues;
						const multipartHeaders: IDataObject = {};
						const neededHeaders: IDataObject = {};
						if (additionalFields.requesterPays) {
							neededHeaders['x-amz-request-payer'] = 'requester';
						}
						if (additionalFields.storageClass) {
							multipartHeaders['x-amz-storage-class'] = snakeCase(
								additionalFields.storageClass as string,
							).toUpperCase();
						}
						if (additionalFields.acl) {
							multipartHeaders['x-amz-acl'] = paramCase(additionalFields.acl as string);
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
							multipartHeaders['x-amz-object-lock-mode'] = additionalFields.lockMode.toUpperCase();
						}
						if (additionalFields.lockRetainUntilDate) {
							multipartHeaders['x-amz-object-lock-retain-until-date'] =
								additionalFields.lockRetainUntilDate;
						}
						if (additionalFields.serverSideEncryption) {
							neededHeaders['x-amz-server-side-encryption'] = additionalFields.serverSideEncryption;
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
						try {
							const stream = new Readable();
							await uploadFileToS3(
								this,
								bucketName,
								stream,
								fileName,
								accessKeyId,
								secretAccessKey,
								region as string,
								multipartHeaders as any,
							);
						} catch (error) {
							throw new NodeOperationError(this.getNode(), error);
						}
					}
				}
			} catch (error) {
				throw new NodeOperationError(this.getNode(), error);
			}
		}
		if (resource === 'file' && operation === 'download') {
			return [items];
		} else {
			return [returnData];
		}
	}
}
