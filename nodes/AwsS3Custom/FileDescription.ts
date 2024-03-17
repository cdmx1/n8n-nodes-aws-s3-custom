import type { INodeProperties } from 'n8n-workflow';

export const fileOperations: INodeProperties[] = [
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
				name: 'Move',
				value: 'move',
				description: 'Move a file',
				action: 'Move a file',
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
];

export const fileFields: INodeProperties[] = [
	/* -------------------------------------------------------------------------- */
	/*                                file:copy                                   */
	/* -------------------------------------------------------------------------- */
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
			'The name of the source bucket should start with (/) and key name of the source object, separated by a slash (/)',
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
		default: {}
	},
	/* -------------------------------------------------------------------------- */
	/*                                file:move                                   */
	/* -------------------------------------------------------------------------- */
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
				operation: ['move'],
			},
		},
		description:
			'The name of the source bucket should start with (/) and key name of the source object, separated by a slash (/)',
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
				operation: ['move'],
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
				operation: ['move'],
			},
		},
		default: {},
		options: [],
	},
	/* -------------------------------------------------------------------------- */
	/*                                file:upload                                 */
	/* -------------------------------------------------------------------------- */
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
		options: [],
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
	/* -------------------------------------------------------------------------- */
	/*                                file:download                               */
	/* -------------------------------------------------------------------------- */
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
	/* -------------------------------------------------------------------------- */
	/*                                file:delete                                 */
	/* -------------------------------------------------------------------------- */
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
	}
];
