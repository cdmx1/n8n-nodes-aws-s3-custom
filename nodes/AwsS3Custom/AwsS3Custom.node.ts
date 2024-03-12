import type { IExecuteFunctions, INodeExecutionData, INodeType, INodeTypeDescription } from 'n8n-workflow';
const crypto_custom = require("crypto");
const change_case_custom = require("change-case");
const xml2js_custom = require("xml2js");
const n8n_workflow_custom = require("n8n-workflow");
const BucketDescription_custom = require("./BucketDescription");
const FolderDescription_custom = require("./FolderDescription");
const FileDescription_custom = require("./FileDescription");
const GenericFunctions_custom = require("./GenericFunctions");
const UPLOAD_CHUNK_SIZE = 5120 * 1024;
interface ObjectWithKey {
	Key: any;
}
interface QueryString {
	prefix?: any;
	'fetch-owner'? : any;
	'list-type'? : any;
	'limit'? : any;
	'versionId'? : any;
	'delimiter'? : any;
}
export class AwsS3Custom implements INodeType {
    description: INodeTypeDescription = {
        displayName: 'AWS S3 Custom',
        name: 'awsS3Custom',
        icon: 'file:s3.svg',
        group: ['output'],
        version: 2,
        subtitle: '={{$parameter["operation"] + ": " + $parameter["resource"]}}',
        description: 'Sends data to AWS S3',
        defaults: {
            name: 'AWS S3 Custom',
        },
        inputs: ['main'],
        outputs: ['main'],
        properties: [
            {
                displayName: 'Region',
                name: 'region',
                type: 'string',
                required: true,
                default: ''
            },
            {
                displayName: 'Access Key ID',
                name: 'accessKeyId',
                type: 'string',
                required: true,
                default: ''
            },
            {
                displayName: 'Secret Access Key',
                name: 'secretAccessKey',
                type: 'string',
                required: true,
                default: '',
								typeOptions: {
									password: true
							}
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
						...BucketDescription_custom.bucketOperations,
						...BucketDescription_custom.bucketFields,
						...FolderDescription_custom.folderOperations,
						...FolderDescription_custom.folderFields,
						...FileDescription_custom.fileOperations,
						...FileDescription_custom.fileFields,
        ],
    };
    execute(this: IExecuteFunctions): Promise<INodeExecutionData[][]> {
			return (async () => {
					try {
            var _a;
            const items = this.getInputData();
            const returnData = [];
            let responseData;
            const region = this.getNodeParameter('region', 0);
            const accessKeyId = this.getNodeParameter('accessKeyId', 0);
            const secretAccessKey = this.getNodeParameter('secretAccessKey', 0);
            const credentials = {
                region: region,
                accessKeyId: accessKeyId,
                secretAccessKey: secretAccessKey,
                temporaryCredentials: false,
                customEndpoints: false
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
                                headers['x-amz-acl'] = (change_case_custom.paramCase)(additionalFields.acl) as string;
                            }
                            if (additionalFields.bucketObjectLockEnabled) {
                                headers['x-amz-bucket-object-lock-enabled'] = additionalFields.bucketObjectLockEnabled as string;
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
                                const builder = new xml2js_custom.Builder();
                                data = builder.buildObject(body);
                            }

                            responseData = await GenericFunctions_custom.awsApiRequestREST.call(this, `${name}.s3`, 'PUT', '', data, qs, headers);

                            const executionData = this.helpers.constructExecutionMetaData(this.helpers.returnJsonArray({ success: true }), { itemData: { item: i } });
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
																	headers['x-amz-storage-class'] = (change_case_custom.snakeCase)(additionalFields.storageClass).toUpperCase();
															}
															responseData = await GenericFunctions_custom.awsApiRequestREST.call(this, servicePath, 'GET', basePath, '', {
																	location: '',
															});
															const region = responseData.LocationConstraint._;
															responseData = await GenericFunctions_custom.awsApiRequestREST.call(this, servicePath, 'PUT', path, '', qs, headers, {}, region);
															const executionData = this.helpers.constructExecutionMetaData(this.helpers.returnJsonArray({ success: true }), { itemData: { item: i } });
															returnData.push(...executionData);
                        }
                        if (operation === 'delete') {
                            const bucketName = this.getNodeParameter('bucketName', i) as string;
															const servicePath = bucketName.includes('.') ? 's3' : `${bucketName}.s3`;
															const basePath = bucketName.includes('.') ? `/${bucketName}` : '';
															const folderKey = this.getNodeParameter('folderKey', i);
															responseData = await GenericFunctions_custom.awsApiRequestREST.call(this, servicePath, 'GET', basePath, '', {
																	location: '',
															});
															const region = responseData.LocationConstraint._;
															responseData = await GenericFunctions_custom.awsApiRequestRESTAllItems.call(this, 'ListBucketResult.Contents', servicePath, 'GET', basePath, '', { 'list-type': 2, prefix: folderKey }, {}, {}, region);
															if (responseData.length === 0) {
																	responseData = await GenericFunctions_custom.awsApiRequestREST.call(this, servicePath, 'DELETE', `${basePath}/${folderKey}`, '', qs, {}, {}, region);
																	responseData = { deleted: [{ Key: folderKey }] };
															}
															else {
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
																	const builder = new xml2js_custom.Builder();
																	const data = builder.buildObject(body);
																	headers['Content-MD5'] = (crypto_custom.createHash)('md5').update(data).digest('base64');
																	headers['Content-Type'] = 'application/xml';
																	responseData = await GenericFunctions_custom.awsApiRequestREST.call(this, servicePath, 'POST', `${basePath}/`, data, { delete: '' }, headers, {}, region);
																	responseData = { deleted: responseData.DeleteResult.Deleted };
															}
															const executionData = this.helpers.constructExecutionMetaData(this.helpers.returnJsonArray(responseData), { itemData: { item: i } });
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
															responseData = await GenericFunctions_custom.awsApiRequestREST.call(this, servicePath, 'GET', basePath, '', {
																	location: '',
															});
															const region = responseData.LocationConstraint._;
															if (returnAll) {
																	responseData = await GenericFunctions_custom.awsApiRequestRESTAllItems.call(this, 'ListBucketResult.Contents', servicePath, 'GET', basePath, '', qs, {}, {}, region);
															}
															else {
																	qs.limit = this.getNodeParameter('limit', 0);
																	responseData = await GenericFunctions_custom.awsApiRequestRESTAllItems.call(this, 'ListBucketResult.Contents', servicePath, 'GET', basePath, '', qs, {}, {}, region);
															}
															if (Array.isArray(responseData)) {
																	responseData = responseData.filter((e) => e.Key.endsWith('/') && e.Size === '0' && e.Key !== options.folderKey);
																	if (qs.limit) {
																			responseData = responseData.splice(0, qs.limit);
																	}
																	const executionData = this.helpers.constructExecutionMetaData(this.helpers.returnJsonArray(responseData), { itemData: { item: i } });
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
                                headers['x-amz-storage-class'] = (change_case_custom.snakeCase)(additionalFields.storageClass).toUpperCase();
                            }
                            if (additionalFields.acl) {
                                headers['x-amz-acl'] = (change_case_custom.paramCase)(additionalFields.acl);
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
                                headers['x-amz-server-side-encryption'] =
                                    additionalFields.serverSideEncryption;
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
                            if (additionalFields.taggingDirective && typeof additionalFields.taggingDirective === 'string') {
                                headers['x-amz-tagging-directive'] = additionalFields.taggingDirective.toUpperCase();
                            }
                            if (additionalFields.metadataDirective && typeof additionalFields.metadataDirective === 'string') {
                                headers['x-amz-metadata-directive'] = additionalFields.metadataDirective.toUpperCase();
                            }
                            const destinationParts = destinationPath.split('/');
                            const bucketName = destinationParts[1];
                            const servicePath = bucketName.includes('.') ? 's3' : `${bucketName}.s3`;
                            const basePath = bucketName.includes('.') ? `/${bucketName}` : '';
                            const destination = `${basePath}/${destinationParts
                                .slice(2, destinationParts.length)
                                .join('/')}`;
                            responseData = await GenericFunctions_custom.awsApiRequestREST.call(this, servicePath, 'GET', basePath, '', {
                                location: '',
                            });
                            const region = responseData.LocationConstraint._;
                            responseData = await GenericFunctions_custom.awsApiRequestREST.call(this, servicePath, 'PUT', destination, '', qs, headers, {}, region);
                            const executionData = this.helpers.constructExecutionMetaData(this.helpers.returnJsonArray(responseData.CopyObjectResult), { itemData: { item: i } });
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
                                throw new n8n_workflow_custom.NodeOperationError(this.getNode(), 'Downloading a whole directory is not yet supported, please provide a file key');
                            }
                            let region = await GenericFunctions_custom.awsApiRequestREST.call(this, servicePath, 'GET', basePath, '', {
                                location: '',
                            });
                            region = region.LocationConstraint._;
                            const response = await GenericFunctions_custom.awsApiRequestREST.call(this, servicePath, 'GET', `${basePath}/${fileKey}`, '', qs, {}, { encoding: null, resolveWithFullResponse: true }, region);
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
															items[i].binary[dataPropertyNameDownload] = await this.helpers.prepareBinaryData(data, fileName, mimeType);
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
                            responseData = await GenericFunctions_custom.awsApiRequestREST.call(this, servicePath, 'GET', basePath, '', {
                                location: '',
                            });
                            const region = responseData.LocationConstraint._;
                            responseData = await GenericFunctions_custom.awsApiRequestREST.call(this, servicePath, 'DELETE', `${basePath}/${fileKey}`, '', qs, {}, {}, region);
                            const executionData = this.helpers.constructExecutionMetaData(this.helpers.returnJsonArray({ success: true }), { itemData: { item: i } });
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
                            responseData = await GenericFunctions_custom.awsApiRequestREST.call(this, servicePath, 'GET', basePath, '', {
                                location: '',
                            });
                            const region = responseData.LocationConstraint._;
                            if (returnAll) {
                                responseData = await GenericFunctions_custom.awsApiRequestRESTAllItems.call(this, 'ListBucketResult.Contents', servicePath, 'GET', basePath, '', qs, {}, {}, region);
                            }
                            else {
                                qs.limit = this.getNodeParameter('limit', 0);
                                responseData = await GenericFunctions_custom.awsApiRequestRESTAllItems.call(this, 'ListBucketResult.Contents', servicePath, 'GET', basePath, '', qs, {}, {}, region);
                                responseData = responseData.splice(0, qs.limit);
                            }
                            if (Array.isArray(responseData)) {
                                responseData = responseData.filter((e) => !e.Key.endsWith('/') && e.Size !== '0');
                                if (qs.limit) {
                                    responseData = responseData.splice(0, qs.limit);
                                }
                                const executionData = this.helpers.constructExecutionMetaData(this.helpers.returnJsonArray(responseData), { itemData: { item: i } });
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
                            const tagsValues = (this.getNodeParameter('tagsUi', i) as { tagsValues?: any })?.tagsValues;
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
                                multipartHeaders['x-amz-storage-class'] = (change_case_custom.snakeCase)(additionalFields.storageClass).toUpperCase();
                            }
                            if (additionalFields.acl) {
                                multipartHeaders['x-amz-acl'] = (change_case_custom.paramCase)(additionalFields.acl);
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
                                multipartHeaders['x-amz-object-lock-legal-hold'] =
                                    additionalFields.lockLegalHold ? 'ON' : 'OFF';
                            }
                            if (additionalFields.lockMode && typeof additionalFields.lockMode === 'string') {
                                multipartHeaders['x-amz-object-lock-mode'] = additionalFields.lockMode.toUpperCase();
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
                            responseData = await GenericFunctions_custom.awsApiRequestREST.call(this, servicePath, 'GET', basePath, '', {
                                location: '',
                            });
                            const region = responseData.LocationConstraint._;
                            if (isBinaryData) {
                                const binaryPropertyName = this.getNodeParameter('binaryPropertyName', i);
                                const binaryPropertyData = this.helpers.assertBinaryData(i, binaryPropertyName);
                                let uploadData;
                                multipartHeaders['Content-Type'] = binaryPropertyData.mimeType;
                                if (binaryPropertyData.id) {
                                    uploadData = await this.helpers.getBinaryStream(binaryPropertyData.id, UPLOAD_CHUNK_SIZE);
                                    const createMultiPartUpload: ReturnType<typeof GenericFunctions_custom.awsApiRequestREST.call> = await GenericFunctions_custom.awsApiRequestREST.call(this, servicePath, 'POST', `${path}?uploads`, body, qs, { ...neededHeaders, ...multipartHeaders }, {}, region);
                                    const uploadId = createMultiPartUpload.InitiateMultipartUploadResult.UploadId;
                                    let part = 1;
                                    for await (const chunk of uploadData) {
                                        const chunkBuffer = await this.helpers.binaryToBuffer(chunk);
                                        const listHeaders = {
                                            'Content-Length': chunk.length,
                                            'Content-MD5': (crypto_custom.createHash)('MD5').update(chunkBuffer).digest('base64'),
                                            ...neededHeaders,
                                        };
                                        try {
                                            await GenericFunctions_custom.awsApiRequestREST.call(this, servicePath, 'PUT', `${path}?partNumber=${part}&uploadId=${uploadId}`, chunk, qs, listHeaders, {}, region);
                                            part++;
                                        }
                                        catch (error) {
                                            try {
                                                await GenericFunctions_custom.awsApiRequestREST.call(this, servicePath, 'DELETE', `${path}?uploadId=${uploadId}`);
                                            }
                                            catch (err) {
                                                throw new n8n_workflow_custom.NodeOperationError(this.getNode(), err);
                                            }
                                            if (((_a = error.response) === null || _a === void 0 ? void 0 : _a.status) !== 308)
                                                throw error;
                                        }
                                    }
                                    const listParts = (await GenericFunctions_custom.awsApiRequestREST.call(this, servicePath, 'GET', `${path}?max-parts=${900}&part-number-marker=0&uploadId=${uploadId}`, '', qs, { ...neededHeaders }, {}, region));
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
                                    }
                                    else {
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
                                    const builder = new xml2js_custom.Builder();
                                    const data = builder.buildObject(body);
                                    const completeUpload = (await GenericFunctions_custom.awsApiRequestREST.call(this, servicePath, 'POST', `${path}?uploadId=${uploadId}`, data, qs, {
                                        ...neededHeaders,
                                        'Content-MD5': (crypto_custom.createHash)('md5').update(data).digest('base64'),
                                        'Content-Type': 'application/xml',
                                    }, {}, region));
                                    responseData = {
                                        ...completeUpload.CompleteMultipartUploadResult,
                                    };
                                }
                                else {
                                    const binaryDataBuffer = await this.helpers.getBinaryDataBuffer(i, binaryPropertyName);
                                    body = binaryDataBuffer;
                                    headers = { ...neededHeaders, ...multipartHeaders };
                                    headers['Content-Type'] = binaryPropertyData.mimeType;
                                    headers['Content-MD5'] = (crypto_custom.createHash)('md5').update(body).digest('base64');
                                    responseData = await GenericFunctions_custom.awsApiRequestREST.call(this, servicePath, 'PUT', path, body, qs, headers, {}, region);
                                }
                                const executionData = this.helpers.constructExecutionMetaData(this.helpers.returnJsonArray(responseData !== null && responseData !== void 0 ? responseData : { success: true }), { itemData: { item: i } });
                                returnData.push(...executionData);
                            }
                            else {
                                const fileContent = this.getNodeParameter('fileContent', i) as any;
                                body = Buffer.from(fileContent, 'utf8');
                                headers = { ...neededHeaders, ...multipartHeaders };
                                headers['Content-Type'] = 'text/html';
                                headers['Content-MD5'] = (crypto_custom.createHash)('md5').update(fileContent).digest('base64');
                                responseData = await GenericFunctions_custom.awsApiRequestREST.call(this, servicePath, 'PUT', path, body, qs, { ...headers }, {}, region);
                                const executionData = this.helpers.constructExecutionMetaData(this.helpers.returnJsonArray({ success: true }), { itemData: { item: i } });
                                returnData.push(...executionData);
                            }
                        }
                    }
                }
                catch (error) {
                    if (this.continueOnFail()) {
                        const executionData = this.helpers.constructExecutionMetaData(this.helpers.returnJsonArray({ error: error.message }), { itemData: { item: i } });
                        returnData.push(...executionData);
                        continue;
                    }
                    throw error;
                }
            }
            if (resource === 'file' && operation === 'download') {
                return [items];
            }
            else {
                return [returnData];
            }
					} catch (error) {
            throw error;
        }
			})();
			}
}

