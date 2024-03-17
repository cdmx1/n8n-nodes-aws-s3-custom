import { S3Client, GetObjectCommand, CopyObjectCommand, DeleteObjectCommand, HeadObjectCommand, PutObjectCommand,DeleteObjectsCommand, ListObjectsCommand, CreateBucketCommand, BucketCannedACL, CompleteMultipartUploadCommand, UploadPartCommand, CreateMultipartUploadCommand } from "@aws-sdk/client-s3";
import type { Readable } from 'stream';
import { createHash } from 'crypto';
import { NodeOperationError } from 'n8n-workflow';

export const regions = [
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
export async function awsGetFile(context: any, bucketName: string, key: string, accessKeyId: any, secretAccessKey: any, region: string) {
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
export async function awsCreateFolder(context: any, bucketName: string, folderName: string, accessKeyId: any, secretAccessKey: any, region: string) {
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
export async function awsDeleteFolder(context: any, bucketName: string, folderName: string, accessKeyId: any, secretAccessKey: any, region: string) {
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
export async function copyFileInS3(context: any, sourceBucketName: any, sourceKey: any, destinationBucketName: any, destinationKey: any, accessKeyId: any, secretAccessKey: any, region: any, options: any) {
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
			ACL: options.acl || undefined, // Apply ACL if provided
			ServerSideEncryption: options.serverSideEncryption || undefined, // Server-side encryption algorithm
			MetadataDirective: options.metadataDirective || undefined, // Metadata directive
			TaggingDirective: options.taggingDirective || undefined, // Tagging directive
			StorageClass: options.storageClass || undefined, // Storage class
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
export async function moveFileInS3(context: any, sourceBucketName: any, sourceKey: any, destinationBucketName: any, destinationKey: any, accessKeyId: any, secretAccessKey: any, region: any, options: any) {
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
			ACL: options.acl || undefined, // Apply ACL if provided
			ServerSideEncryption: options.serverSideEncryption || undefined, // Server-side encryption algorithm
			MetadataDirective: options.metadataDirective || undefined, // Metadata directive
			TaggingDirective: options.taggingDirective || undefined, // Tagging directive
			StorageClass: options.storageClass || undefined, // Storage class
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
export async function deleteFileInS3(context: any, bucketName: any, key: any, accessKeyId: any, secretAccessKey: any, region: any) {
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

export async function createS3Bucket(context: any, bucketName: string, accessKeyId: any, secretAccessKey: any, region: string, options: { acl?: BucketCannedACL } = {}) {
	const { acl = 'private' } = options;

	const client = new S3Client({
			credentials: {
					accessKeyId,
					secretAccessKey,
			},
			region,
	});

	try {
			const createBucketCommand: CreateBucketCommand = new CreateBucketCommand({
					Bucket: bucketName,
					ACL: acl,
			});

			await client.send(createBucketCommand);
			return {success: true};
	} catch (error) {
			throw new NodeOperationError(
					context.getNode(),
					error
			);
	}
}
export async function uploadStreamToS3(
	context: any,
	bucketName: string,
	accessKeyId: any,
	secretAccessKey: any,
	region: string,
	data: Buffer | Readable,
	key: string,
	neededHeaders: { [key: string]: string },
	options: any
) {
	const client = new S3Client({
			credentials: {
					accessKeyId,
					secretAccessKey,
			},
			region,
	});

	try {
			// Initiate multipart upload
			const createMultiPartUpload = await client.send(new CreateMultipartUploadCommand({
					Bucket: bucketName,
					Key: key,
					ACL: options.acl || undefined, // Apply ACL if provided
					ServerSideEncryption: options.serverSideEncryption || undefined, // Server-side encryption algorithm
					...neededHeaders,
			}));
			const uploadId = createMultiPartUpload.UploadId;

			// Upload parts
			let partNumber = 1;
			const parts = [];
			if (data instanceof Buffer) {
					// If data is a Buffer, upload it directly
					const contentMD5 = createHash('md5').update(data).digest('base64');
					const uploadPartResult = await client.send(new UploadPartCommand({
							Bucket: bucketName,
							Key: key,
							UploadId: uploadId,
							PartNumber: partNumber,
							Body: data,
							ContentMD5: contentMD5,
					}));
					parts.push({
							PartNumber: partNumber,
							ETag: uploadPartResult.ETag,
					});
			} else {
					// If data is a Readable stream, upload it in chunks
					for await (const chunk of data) {
							const chunkBuffer = chunk instanceof Buffer ? chunk : Buffer.from(chunk);
							const contentMD5 = createHash('md5').update(chunkBuffer).digest('base64');
							const uploadPartResult = await client.send(new UploadPartCommand({
									Bucket: bucketName,
									Key: key,
									UploadId: uploadId,
									PartNumber: partNumber,
									Body: chunkBuffer,
									ContentMD5: contentMD5,
							}));
							parts.push({
									PartNumber: partNumber,
									ETag: uploadPartResult.ETag,
							});
							partNumber++;
					}
			}

			// Complete multipart upload
			const completeResult = await client.send(new CompleteMultipartUploadCommand({
					Bucket: bucketName,
					Key: key,
					UploadId: uploadId,
					MultipartUpload: { Parts: parts },
			}));
			return completeResult;
	} catch (error) {
			throw new NodeOperationError(
					context.getNode(),
					error
			);
	}
}
