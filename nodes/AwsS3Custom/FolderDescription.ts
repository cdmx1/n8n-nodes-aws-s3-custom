import type { INodeProperties } from 'n8n-workflow';

export const folderOperations: INodeProperties[] = [
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
];

export const folderFields: INodeProperties[] = [
	/* -------------------------------------------------------------------------- */
	/*                                folder:create                               */
	/* -------------------------------------------------------------------------- */
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
	}
];
