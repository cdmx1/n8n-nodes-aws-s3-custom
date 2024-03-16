import type { INodeProperties } from 'n8n-workflow';

export const bucketOperations: INodeProperties[] = [
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
];

export const bucketFields: INodeProperties[] = [
	/* -------------------------------------------------------------------------- */
	/*                                bucket:create                               */
	/* -------------------------------------------------------------------------- */
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
		description: 'A succinct description of the nature, symptoms, cause, or effect of the bucket',
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
				default: 'publicRead',
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
	/* -------------------------------------------------------------------------- */
	/*                                 bucket:getAll                              */
	/* -------------------------------------------------------------------------- */
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
			minValue: 1
		},
		default: 50,
		description: 'Max number of results to return',
	}
];
