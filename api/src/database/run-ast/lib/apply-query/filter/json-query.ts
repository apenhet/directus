import type { Knex } from 'knex';
import type { SchemaOverview } from '@directus/types';
import { isObject } from '@directus/utils';
import { getFilterPath } from '../get-filter-path.js';
import { getOperation } from '../get-operation.js';
import { generateAlias } from '../../../utils/generate-alias.js';

export function getJsonOperatorAndValues(filterOperator: string, filterValue: any): { operator: string; values: any[] } {
	const values: any[] = [];

	switch (filterOperator) {
		case '_contains':
		case '_ncontains':
		case '_icontains':
		case '_nicontains':
			values.push(`%${filterValue}%`);
			break;
		case '_starts_with':
			values.push(`${filterValue}%`);
			break;
		case '_ends_with':
			values.push(`%${filterValue}`);
			break;
		case '_in':
		case '_nin':
			values.push(...(Array.isArray(filterValue) ? filterValue : filterValue.split(',')));
			break;
		case '_gt':
		case '_gte':
		case '_lt':
		case '_lte':
			values.push(Number.isFinite(parseFloat(filterValue)) ? parseFloat(filterValue) : filterValue);
			break;
		case '_between':
		case '_nbetween':
			values.push(...[filterValue[0] ??= 0, filterValue[1] ?? filterValue[0]]);
			break;
		case '_empty':
		case '_nempty':
			break;
		default:
			values.push(filterValue);
	}

	const operator = {
		'_eq': `= ?`,
		'_neq': `!= ?`,
		'_contains': `LIKE ?`,
		'_ncontains': `NOT LIKE ?`,
		'_icontains': `LIKE ?`,
		'_nicontains': `NOT LIKE ?`,
		'_starts_with': `LIKE ?`,
		'_ends_with': `LIKE ?`,
		'_gt': `> ?`,
		'_gte': `>= ?`,
		'_lt': `< ?`,
		'_lte': `<= ?`,
		'_in': `IN (${values.map(() => `?`).join(', ')})`,
		'_nin': `NOT IN (${values.map(() => `?`).join(', ')})`,
		'_empty': 'IS NULL',
		'_nempty': 'IS NOT NULL',
		'_between': `BETWEEN ? AND ?`,
		'_nbetween': `NOT BETWEEN ? AND ?`,
	}[filterOperator] ?? '= ?';

	return { operator, values };
}

export function buildJsonPath(filterPath: string[]) {
	const keys = filterPath
		.slice(1)
		.map((segment) => segment.match(/^\[(.+)\]$/)?.[1] ?? segment)
		.filter((segment) => segment.startsWith('_') === false);

	const tokens: string[] = ['$'];

	keys.forEach((key, index) => {
		if (/^[A-Za-z_][A-Za-z0-9_]*$/.test(key)) {
			tokens.push(`.${key}`);
		} else {
			const escaped = key.replace(/"/g, '\\"');
			tokens.push(`."${escaped}"`);
		}

		if (index < keys.length - 1) {
			tokens.push('[*]');
		}
	});

	return tokens.join('');
}

export function addJsonQuery(query: Knex.QueryBuilder, filterPath: string[], filterOperator: string, filterValue: any, alias: string) {
	const cast = Number.isFinite(parseFloat(filterValue)) ? 'float' : 'text';

	const jsonPath = buildJsonPath(filterPath);
	const { operator, values } = getJsonOperatorAndValues(filterOperator, filterValue);

	const jsonValueAccessor = `(json_value #>> '{}')`;

	query.whereExists((subQuery) => {
		subQuery
			.select(query.client.raw('1'))
			.fromRaw(query.client.raw(`jsonb_path_query((${alias})::jsonb, ?) as json_path_value(json_value)`, [jsonPath]));

		subQuery.whereRaw(`(${jsonValueAccessor})::${cast} ${operator}`, values);
	});
}

export function handleJsonFieldWithAndOr(
	query: Knex.QueryBuilder,
	key: string,
	value: Record<string, any>,
	alias: string,
	logical: 'and' | 'or',
) {
	const nestedFilters = Object.entries(value);

	if (nestedFilters.length === 0) return;

	for (const [nestedKey, nestedValue] of nestedFilters) {
		if (nestedKey === '_and' || nestedKey === '_or') {
			// Handle _and/_or inside JSON field filters
			const nestedLogical = nestedKey === '_and' ? 'and' : 'or';

			if (nestedKey === '_and') {
				// For _and, try to combine conditions on the same path
				const conditions: Array<{
					path: string[];
					operator: string;
					value: any;
					field: string;
				}> = [];

				(nestedValue as Record<string, any>[]).forEach((subFilter: Record<string, any>) => {
					for (const [subKey, subValue] of Object.entries(subFilter)) {
						const subOperation = getOperation(subKey, subValue);

						if (!subOperation) continue;

						const subPath = [key, ...getFilterPath(subKey, subValue as Record<string, any>)];
						const field = subPath[subPath.length - 1]!;

						conditions.push({
							path: subPath,
							operator: subOperation.operator,
							value: subOperation.value,
							field,
						});
					}
				});

				// Group conditions by base path (everything except the last field)
				const groupedConditions = new Map<string, typeof conditions>();

				for (const condition of conditions) {
					const basePath = condition.path.slice(0, -1).join('.');

					if (!groupedConditions.has(basePath)) {
						groupedConditions.set(basePath, []);
					}

					groupedConditions.get(basePath)!.push(condition);
				}

				// For each group, create combined query if multiple conditions, otherwise single query
				for (const [_basePathKey, groupConditions] of groupedConditions) {
					if (groupConditions.length > 1) {
						// Multiple conditions on same path - combine into single query
						const basePath = groupConditions[0]!.path.slice(0, -1);
						const jsonPath = buildJsonPath(basePath);

						query.whereExists((jsonSubQuery: Knex.QueryBuilder) => {
							jsonSubQuery
								.select(query.client.raw('1'))
								.fromRaw(
									query.client.raw(
										`jsonb_path_query((${alias})::jsonb, ?) as json_path_value(json_value)`,
										[jsonPath],
									),
								);

							const whereConditions: string[] = [];
							const bindings: any[] = [];

							for (const condition of groupConditions) {
								const cast = Number.isFinite(parseFloat(condition.value)) ? 'float' : 'text';
								const { operator, values } = getJsonOperatorAndValues(condition.operator, condition.value);

								whereConditions.push(`(jsonb_extract_path_text(json_value, ?)::${cast} ${operator})`);
								bindings.push(condition.field, ...values);
							}

							jsonSubQuery.whereRaw(whereConditions.join(' AND '), bindings);
						});
					} else {
						// Single condition - use regular query
						const condition = groupConditions[0]!;
						addJsonQuery(query[logical], condition.path, condition.operator, condition.value, alias);
					}
				}
			} else {
				// For _or, use separate queries
				(nestedValue as Record<string, any>[]).forEach((subFilter: Record<string, any>) => {
					for (const [subKey, subValue] of Object.entries(subFilter)) {
						const subOperation = getOperation(subKey, subValue);

						if (!subOperation) continue;

						const subPath = [key, ...getFilterPath(subKey, subValue as Record<string, any>)];

						addJsonQuery(query[nestedLogical], subPath, subOperation.operator, subOperation.value, alias);
					}
				});
			}

			continue;
		}

		const nestedOperation = getOperation(nestedKey, nestedValue);

		if (!nestedOperation) continue;

		const nestedPath = [key, ...getFilterPath(nestedKey, nestedValue as Record<string, any>)];

		addJsonQuery(query[logical], nestedPath, nestedOperation.operator, nestedOperation.value, alias);
	}
}

export function handleCombinedJsonConditionsInAnd(
	knex: Knex,
	subFilters: Record<string, any>[],
	collection: string,
	schema: SchemaOverview,
	jsonQueries: Map<string, { alias: string; query: Knex.QueryBuilder }>,
	subQuery: Knex.QueryBuilder,
) {
	// For _and, try to combine JSON field filters that target the same field and base path
	const jsonConditions = new Map<string, Array<{ field: string; path: string[]; operator: string; value: any }>>();

	// Collect all JSON field conditions
	for (const subFilter of subFilters) {
		for (const [subKey, subValue] of Object.entries(subFilter)) {
			const fieldInfo = schema.collections[collection]?.fields[subKey];

			if (fieldInfo?.type === 'json' && isObject(subValue)) {
				const nestedPath = getFilterPath(subKey, subValue);

				if (nestedPath.length > 1) {
					const operation = getOperation(nestedPath[1]!, Object.values(subValue)[0] as Record<string, any>);

					if (operation) {
						const fullPath = [subKey, ...nestedPath.slice(1)];
						const basePath = fullPath.slice(0, -1);
						const basePathKey = `${subKey}.${basePath.slice(1).join('.')}`;
						const field = fullPath[fullPath.length - 1]!;

						if (!jsonConditions.has(basePathKey)) {
							jsonConditions.set(basePathKey, []);
						}

						jsonConditions.get(basePathKey)!.push({
							field,
							path: fullPath,
							operator: operation.operator,
							value: operation.value,
						});
					}
				}
			}
		}
	}

	// Handle combined JSON conditions
		for (const [basePathKey, conditions] of jsonConditions) {
		if (conditions.length > 1) {
			// Multiple conditions on same base path - combine them
			const [fieldName, ...basePathParts] = basePathKey.split('.');

			if (!fieldName) continue;

				const basePath = [fieldName, ...basePathParts.filter((p): p is string => p !== undefined)];
				// We want to target individual items in the array at this base path
				const jsonPath = `${buildJsonPath(basePath)}[*]`;

			const aliasKey = `${collection}.${fieldName}`;

			if (!jsonQueries.has(aliasKey)) {
				const alias = generateAlias();
				const query = knex.select('*').from(knex.raw(`json_array_elements(??.??) as ${alias}`, [collection, fieldName]));

				jsonQueries.set(aliasKey, {
					alias,
					query,
				});

				subQuery.whereExists(query);
			}

			const { query, alias } = jsonQueries.get(aliasKey)!;

			query.whereExists((jsonSubQuery: Knex.QueryBuilder) => {
				jsonSubQuery
					.select(query.client.raw('1'))
					.fromRaw(
						query.client.raw(
							`jsonb_path_query((${alias})::jsonb, ?) as json_path_value(json_value)`,
							[jsonPath],
						),
					);

				const whereConditions: string[] = [];
				const bindings: any[] = [];

				for (const condition of conditions) {
					const cast = Number.isFinite(parseFloat(condition.value)) ? 'float' : 'text';
					const { operator, values } = getJsonOperatorAndValues(condition.operator, condition.value);

					whereConditions.push(`(jsonb_extract_path_text(json_value, ?)::${cast} ${operator})`);
					bindings.push(condition.field, ...values);
				}

				jsonSubQuery.whereRaw(whereConditions.join(' AND '), bindings);
			});
		}
	}

	return jsonConditions;
}

