import { SchemaBuilder } from '@directus/schema-builder';
import knex from 'knex';
import { expect, test, vi } from 'vitest';
import { Client_SQLite3 } from '../mock.js';

const JSON_ALIAS = 'jsonal';

vi.mock('../../../utils/generate-alias.js', async () => {
	const actual = await vi.importActual<typeof import('../../../utils/generate-alias.js')>(
		'../../../utils/generate-alias.js',
	);

	return {
		...actual,
		generateAlias: vi.fn((context?: string) => {
			if (context) {
				return actual.generateAlias(context);
			}

			return JSON_ALIAS;
		}),
	};
});

const { applyFilter } = await import('./index.js');

test(`filtering nested json query`, async () => {
	const schema = new SchemaBuilder()
		.collection('test', (c) => {
			c.field('id').id();
			c.field('name').string();
			c.field('activities').json();
		})
		.build();

	const db = vi.mocked(knex.default({ client: Client_SQLite3 }));
	const queryBuilder = db.queryBuilder();

	applyFilter(
		db,
		schema,
		queryBuilder,
		{
			activities: {
				beneficiaries: {
					deliverables: {
						type: {
							_eq: 'deliverable_1',
						},
					},
				},
			},
		},
		'test',
		{},
		[],
		[],
	);

	const rawQuery = queryBuilder.toSQL();

	expect(rawQuery.sql).toEqual(
		`select * where exists (select * from json_array_elements("test"."activities") as ${JSON_ALIAS} where exists (select 1 from jsonb_path_query((${JSON_ALIAS})::jsonb, ?) as json_path_value(json_value) where ((json_value #>> '{}'))::text = ?))`,
	);

	expect(rawQuery.bindings).toEqual(['$.beneficiaries[*].deliverables[*].type', 'deliverable_1']);
});

test(`filtering nested json query with logical operators`, async () => {
	const schema = new SchemaBuilder()
		.collection('test', (c) => {
			c.field('id').id();
			c.field('name').string();
			c.field('activities').json();
		})
		.build();

	const db = vi.mocked(knex.default({ client: Client_SQLite3 }));
	const queryBuilder = db.queryBuilder();

	applyFilter(
		db,
		schema,
		queryBuilder,
		{
			_and: [
				{
					name: {
						_eq: 'cool',
					},
				},
				{
					_or: [
						{
							activities: {
								beneficiaries: {
									deliverables: {
										type: {
											_eq: 'deliverable_1',
										},
									},
								},
							},
						},
						{
							activities: {
								beneficiaries: {
									type: {
										_eq: 'type_2',
									},
								},
							},
						},
					],
				},
			],
		},
		'test',
		{},
		[],
		[],
	);

	const rawQuery = queryBuilder.toSQL();

	expect(rawQuery.sql).toEqual(
		`select * where ("test"."name" = ? and (exists (select * from json_array_elements("test"."activities") as ${JSON_ALIAS} where exists (select 1 from jsonb_path_query((${JSON_ALIAS})::jsonb, ?) as json_path_value(json_value) where ((json_value #>> '{}'))::text = ?) or exists (select 1 from jsonb_path_query((${JSON_ALIAS})::jsonb, ?) as json_path_value(json_value) where ((json_value #>> '{}'))::text = ?))))`,
	);

	expect(rawQuery.bindings).toEqual([
		'cool',
		'$.beneficiaries[*].deliverables[*].type',
		'deliverable_1',
		'$.beneficiaries[*].type',
		'type_2',
	]);
});

test(`filtering json query with sibling arrays`, async () => {
	const schema = new SchemaBuilder()
		.collection('test', (c) => {
			c.field('id').id();
			c.field('name').string();
			c.field('activities').json();
		})
		.build();

	const db = vi.mocked(knex.default({ client: Client_SQLite3 }));
	const queryBuilder = db.queryBuilder();

	applyFilter(
		db,
		schema,
		queryBuilder,
		{
			activities: {
				beneficiaries: {
					type: {
						_eq: 'type_2',
					},
				},
				deliverables: {
					type: {
						_eq: 'deliverable_3',
					},
				},
			},
		},
		'test',
		{},
		[],
		[],
	);

	const rawQuery = queryBuilder.toSQL();

	expect(rawQuery.sql).toEqual(
		`select * where exists (select * from json_array_elements("test"."activities") as ${JSON_ALIAS} where exists (select 1 from jsonb_path_query((${JSON_ALIAS})::jsonb, ?) as json_path_value(json_value) where ((json_value #>> '{}'))::text = ?) and exists (select 1 from jsonb_path_query((${JSON_ALIAS})::jsonb, ?) as json_path_value(json_value) where ((json_value #>> '{}'))::text = ?))`,
	);

	expect(rawQuery.bindings).toEqual([
		'$.beneficiaries[*].type',
		'type_2',
		'$.deliverables[*].type',
		'deliverable_3',
	]);
});

