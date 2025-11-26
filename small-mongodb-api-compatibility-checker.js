const { MongoClient, Timestamp } = require('mongodb');

const uri = process.env.MONGO_URI || 'mongodb://localhost:27017';
const dbName = 'aggregation_test';
const results = { passed: [], failed: [] };

async function runTests() {
  const client = new MongoClient(uri);
  try {
    await client.connect();
    const db = client.db(dbName);
    const adminDb = client.db("admin");
    const coll = db.collection('test');
    
    await coll.deleteMany({});
    await coll.insertMany([
      { _id: 1, name: 'Alice', age: 25, score: 85, dept: 'Sales', salary: 50000, tags: ['a', 'b'], date: new Date('2024-01-15'), items: [{ qty: 5, price: 10 }], location: { type: "Point", coordinates: [-72.11, 35.66] }, ts: new Timestamp()},
      { _id: 2, name: 'Bob', age: 30, score: 90, dept: 'IT', salary: 60000, tags: ['b', 'c'], date: new Date('2024-02-20'), items: [{ qty: 3, price: 15 }], location: { type: "Point", coordinates: [-71.12, 24.86] }, ts: new Timestamp()},
      { _id: 3, name: 'Charlie', age: 35, score: 75, dept: 'Sales', salary: 55000, tags: ['a', 'c'], date: new Date('2024-03-10'), items: [{ qty: 8, price: 12 }], location: { type: "Point", coordinates: [-74.44, 32.44] }, ts: new Timestamp()},
      { _id: 4, name: 'Diana', age: 28, score: 95, dept: 'IT', salary: 65000, tags: ['d'], date: new Date('2024-04-05'), items: [{ qty: 2, price: 20 }], location: { type: "Point", coordinates: [-76.21, 31.55] }, ts: new Timestamp()}
    ]);
    await coll.createIndex({ location: "2dsphere" });

    // AGGREGATION STAGES
    await test('$addFields', () => coll.aggregate([{ $addFields: { newField: 'test' } }]).toArray());
    await test('$bucket', () => coll.aggregate([{ $bucket: { groupBy: '$age', boundaries: [20, 30, 40], default: 'Other' } }]).toArray());
    await test('$bucketAuto', () => coll.aggregate([{ $bucketAuto: { groupBy: '$score', buckets: 2 } }]).toArray());
    await test('$changeStream', () => Promise.resolve([]), true);
    await test('$changeStreamSplitLargeEvent', () => Promise.resolve([]), true);
    await test('$collStats', () => coll.aggregate([{ $collStats: { latencyStats: {} } }]).toArray());
    await test('$currentOp', () => adminDb.aggregate([{ $currentOp: {} }]).toArray());
    await test('$count', () => coll.aggregate([{ $count: 'total' }]).toArray());
    await test('$densify', () => coll.aggregate([{ $densify: { field: 'age', range: { step: 1, bounds: [25, 35] } } }]).toArray());
    await test('$documents', () => db.aggregate([{ $documents: [{ x: 1 }, { x: 2 }] }]).toArray());
    await test('$facet', () => coll.aggregate([{ $facet: { byDept: [{ $group: { _id: '$dept', count: { $sum: 1 } } }], byAge: [{ $group: { _id: '$age' } }] } }]).toArray());
    await test('$fill', () => coll.aggregate([{ $fill: { sortBy: { age: 1 }, output: { score: { method: 'linear' } } } }]).toArray());
    await test('$geoNear', () =>coll.aggregate([{$geoNear: {near: { type: "Point", coordinates: [-73.97, 40.77] },distanceField: "distance", spherical: true, maxDistance: 1000}}]).toArray());
    await test('$graphLookup', () => coll.aggregate([{ $graphLookup: { from: 'test', startWith: '$_id', connectFromField: '_id', connectToField: '_id', as: 'connections' } }]).toArray());
    await test('$group', () => coll.aggregate([{ $group: { _id: '$dept', total: { $sum: 1 } } }]).toArray());
    await test('$indexStats', () => coll.aggregate([{ $indexStats: {} }]).toArray());
    await test('$limit', () => coll.aggregate([{ $limit: 2 }]).toArray());
    await test('$listCachedAndActiveUsers', () => db.aggregate([{ $listCachedAndActiveUsers: {} }]).toArray());
    await test('$listCatalog', () => adminDb.aggregate([{ $listCatalog: {aggregate: 1} }]).toArray());
    await test('$listLocalSessions', () => db.aggregate([{ $listLocalSessions: {} }]).toArray());
    await test('$listSampledQueries', () => adminDb.aggregate([{ $listSampledQueries: {aggregate: 1} }]).toArray());
    await test('$listSearchIndexes', () => coll.aggregate([{ $listSearchIndexes: {} }]).toArray());
    await test('$listSessions', () => db.aggregate([{ $listSessions: {} }]).toArray());
    await test('$lookup', () => coll.aggregate([{ $lookup: { from: 'test', localField: 'dept', foreignField: 'dept', as: 'colleagues' } }]).toArray());
    await test('$mergeCursors', () => Promise.resolve([]), true);
    await test('$match', () => coll.aggregate([{ $match: { age: { $gt: 25 } } }]).toArray());
    await test('$operationMetrics', () => db.aggregate([{ $operationMetrics: {} }]).toArray());
    await test('$planCacheStats', () => coll.aggregate([{ $planCacheStats: {} }]).toArray());
    await test('$querySettings', () => db.aggregate([{ $querySettings: {} }]).toArray());
    await test('$queryStats', () => db.aggregate([{ $queryStats: {} }]).toArray());
    await test('$queue', () => Promise.resolve([]), true);
    await test('$merge', () => coll.aggregate([{ $merge: { into: 'output', whenMatched: 'replace' } }]).toArray());
    await test('$out', () => coll.aggregate([{ $out: 'output2' }]).toArray());
    await test('$project', () => coll.aggregate([{ $project: { name: 1, age: 1 } }]).toArray());
    await test('$redact', () => coll.aggregate([{ $redact: '$$KEEP' }]).toArray());
    await test('$replaceRoot', () => coll.aggregate([{ $replaceRoot: { newRoot: { name: '$name', age: '$age' } } }]).toArray());
    await test('$replaceWith', () => coll.aggregate([{ $replaceWith: { name: '$name' } }]).toArray());
    await test('$sample', () => coll.aggregate([{ $sample: { size: 2 } }]).toArray());
    await test('$search', () => Promise.resolve([]), true);
    await test('$searchBeta', () => Promise.resolve([]), true);
    await test('$searchMeta', () => Promise.resolve([]), true);
    await test('$set', () => coll.aggregate([{ $set: { computed: { $multiply: ['$age', 2] } } }]).toArray());
    await test('$setVariableFromSubPipeline', () => Promise.resolve([]), true);
    await test('$setWindowFields', () => coll.aggregate([{ $setWindowFields: { sortBy: { age: 1 }, output: { rank: { $rank: {} } } } }]).toArray());
    await test('$shardedDataDistribution', () => db.aggregate([{ $shardedDataDistribution: {} }]).toArray());
    await test('$skip', () => coll.aggregate([{ $skip: 1 }]).toArray());
    await test('$sort', () => coll.aggregate([{ $sort: { age: -1 } }]).toArray());
    await test('$sortByCount', () => coll.aggregate([{ $sortByCount: '$dept' }]).toArray());
    await test('$unionWith', () => coll.aggregate([{ $unionWith: 'test' }]).toArray());
    await test('$unset', () => coll.aggregate([{ $unset: 'tags' }]).toArray());
    await test('$unwind', () => coll.aggregate([{ $unwind: '$tags' }]).toArray());
    await test('$vectorSearch', () => Promise.resolve([]), true);

    // OPERATORS - Arithmetic
    await test('$abs', () => coll.aggregate([{ $project: { result: { $abs: -5 } } }]).toArray());
    await test('$acos', () => coll.aggregate([{ $project: { result: { $acos: 0.5 } } }]).toArray());
    await test('$acosh', () => coll.aggregate([{ $project: { result: { $acosh: 2 } } }]).toArray());
    await test('$add', () => coll.aggregate([{ $project: { result: { $add: ['$age', 5] } } }]).toArray());
    await test('$asin', () => coll.aggregate([{ $project: { result: { $asin: 0.5 } } }]).toArray());
    await test('$asinh', () => coll.aggregate([{ $project: { result: { $asinh: 1 } } }]).toArray());
    await test('$atan', () => coll.aggregate([{ $project: { result: { $atan: 1 } } }]).toArray());
    await test('$atan2', () => coll.aggregate([{ $project: { result: { $atan2: [1, 1] } } }]).toArray());
    await test('$atanh', () => coll.aggregate([{ $project: { result: { $atanh: 0.5 } } }]).toArray());
    await test('$ceil', () => coll.aggregate([{ $project: { result: { $ceil: 4.2 } } }]).toArray());
    await test('$cos', () => coll.aggregate([{ $project: { result: { $cos: 1 } } }]).toArray());
    await test('$cosh', () => coll.aggregate([{ $project: { result: { $cosh: 1 } } }]).toArray());
    await test('$degreesToRadians', () => coll.aggregate([{ $project: { result: { $degreesToRadians: 180 } } }]).toArray());
    await test('$divide', () => coll.aggregate([{ $project: { result: { $divide: ['$score', 10] } } }]).toArray());
    await test('$exp', () => coll.aggregate([{ $project: { result: { $exp: 2 } } }]).toArray());
    await test('$floor', () => coll.aggregate([{ $project: { result: { $floor: 4.8 } } }]).toArray());
    await test('$ln', () => coll.aggregate([{ $project: { result: { $ln: 10 } } }]).toArray());
    await test('$log', () => coll.aggregate([{ $project: { result: { $log: [10, 2] } } }]).toArray());
    await test('$log10', () => coll.aggregate([{ $project: { result: { $log10: 100 } } }]).toArray());
    await test('$median', () => coll.aggregate([{ $project: { result: { $median: {input: ['$age', 40, 45, 50], method: "approximate" } } } }]).toArray());
    await test('$mod', () => coll.aggregate([{ $project: { result: { $mod: ['$age', 10] } } }]).toArray());
    await test('$multiply', () => coll.aggregate([{ $project: { result: { $multiply: ['$age', 2] } } }]).toArray());
    await test('$pow', () => coll.aggregate([{ $project: { result: { $pow: [2, 3] } } }]).toArray());
    await test('$round', () => coll.aggregate([{ $project: { result: { $round: [4.567, 2] } } }]).toArray());
    await test('$sqrt', () => coll.aggregate([{ $project: { result: { $sqrt: 16 } } }]).toArray());
    await test('$subtract', () => coll.aggregate([{ $project: { result: { $subtract: ['$age', 5] } } }]).toArray());
    await test('$trunc', () => coll.aggregate([{ $project: { result: { $trunc: [4.567, 1] } } }]).toArray());
    await test('$radiansToDegrees', () => coll.aggregate([{ $project: { result: { $radiansToDegrees: 3.14159 } } }]).toArray());
    await test('$sin', () => coll.aggregate([{ $project: { result: { $sin: 1 } } }]).toArray());
    await test('$sinh', () => coll.aggregate([{ $project: { result: { $sinh: 1 } } }]).toArray());
    await test('$tan', () => coll.aggregate([{ $project: { result: { $tan: 1 } } }]).toArray());
    await test('$tanh', () => coll.aggregate([{ $project: { result: { $tanh: 1 } } }]).toArray());

    // OPERATORS - Array
    await test('$allElementsTrue', () => coll.aggregate([{ $project: { result: { $allElementsTrue: [[true, true, true]] } } }]).toArray());
    await test('$anyElementTrue', () => coll.aggregate([{ $project: { result: { $anyElementTrue: [[false, true, false]] } } }]).toArray());
    await test('$arrayElemAt', () => coll.aggregate([{ $project: { result: { $arrayElemAt: ['$tags', 0] } } }]).toArray());
    await test('$arrayToObject', () => coll.aggregate([{ $project: { result: { $arrayToObject: [[{ k: 'a', v: 1 }]] } } }]).toArray());
    await test('$concatArrays', () => coll.aggregate([{ $project: { result: { $concatArrays: ['$tags', ['x']] } } }]).toArray());
    await test('$filter', () => coll.aggregate([{ $project: { result: { $filter: { input: '$tags', as: 't', cond: { $eq: ['$$t', 'a'] } } } } }]).toArray());
    await test('$first', () => coll.aggregate([{ $project: { result: { $first: '$tags' } } }]).toArray());
    await test('$firstN', () => coll.aggregate([{ $project: { result: { $firstN: { input: '$tags', n: 1 } } } }]).toArray());
    await test('$in', () => coll.aggregate([{ $project: { result: { $in: ['a', '$tags'] } } }]).toArray());
    await test('$indexOfArray', () => coll.aggregate([{ $project: { result: { $indexOfArray: ['$tags', 'b'] } } }]).toArray());
    await test('$isArray', () => coll.aggregate([{ $project: { result: { $isArray: '$tags' } } }]).toArray());
    await test('$last', () => coll.aggregate([{ $project: { result: { $last: '$tags' } } }]).toArray());
    await test('$lastN', () => coll.aggregate([{ $project: { result: { $lastN: { input: '$tags', n: 1 } } } }]).toArray());
    await test('$map', () => coll.aggregate([{ $project: { result: { $map: { input: '$tags', as: 't', in: { $toUpper: '$$t' } } } } }]).toArray());
    await test('$maxN', () => coll.aggregate([{ $project: { result: { $maxN: { input: '$tags', n: 2 } } } }]).toArray());
    await test('$minN', () => coll.aggregate([{ $project: { result: { $minN: { input: '$tags', n: 2 } } } }]).toArray());
    await test('$objectToArray', () => coll.aggregate([{ $project: { result: { $objectToArray: { a: 1, b: 2 } } } }]).toArray());
    await test('$range', () => coll.aggregate([{ $project: { result: { $range: [0, 5] } } }]).toArray());
    await test('$reduce', () => coll.aggregate([{ $project: { result: { $reduce: { input: '$tags', initialValue: '', in: { $concat: ['$$value', '$$this'] } } } } }]).toArray());
    await test('$reverseArray', () => coll.aggregate([{ $project: { result: { $reverseArray: '$tags' } } }]).toArray());
    await test('$size', () => coll.aggregate([{ $project: { result: { $size: '$tags' } } }]).toArray());
    await test('$slice', () => coll.aggregate([{ $project: { result: { $slice: ['$tags', 1] } } }]).toArray());
    await test('$sortArray', () => coll.aggregate([{ $project: { result: { $sortArray: { input: '$tags', sortBy: 1 } } } }]).toArray());
    await test('$zip', () => coll.aggregate([{ $project: { result: { $zip: { inputs: [['a', 'b'], [1, 2]] } } } }]).toArray());

    // OPERATORS - Set
    await test('$setDifference', () => coll.aggregate([{ $project: { result: { $setDifference: [['a', 'b', 'c'], ['b', 'd']] } } }]).toArray());
    await test('$setEquals', () => coll.aggregate([{ $project: { result: { $setEquals: [['a', 'b'], ['b', 'a']] } } }]).toArray());
    await test('$setIntersection', () => coll.aggregate([{ $project: { result: { $setIntersection: [['a', 'b'], ['b', 'c']] } } }]).toArray());
    await test('$setIsSubset', () => coll.aggregate([{ $project: { result: { $setIsSubset: [['a', 'b'], ['a', 'b', 'c']] } } }]).toArray());
    await test('$setUnion', () => coll.aggregate([{ $project: { result: { $setUnion: [['a', 'b'], ['b', 'c']] } } }]).toArray());

    // OPERATORS - Bitwise
    await test('$bitAnd', () => coll.aggregate([{ $project: { result: { $bitAnd: [5, 3] } } }]).toArray());
    await test('$bitNot', () => coll.aggregate([{ $project: { result: { $bitNot: 5 } } }]).toArray());
    await test('$bitOr', () => coll.aggregate([{ $project: { result: { $bitOr: [5, 3] } } }]).toArray());
    await test('$bitXor', () => coll.aggregate([{ $project: { result: { $bitXor: [5, 3] } } }]).toArray());

    // OPERATORS - Boolean
    await test('$and', () => coll.aggregate([{ $project: { result: { $and: [true, true] } } }]).toArray());
    await test('$not', () => coll.aggregate([{ $project: { result: { $not: [false] } } }]).toArray());
    await test('$or', () => coll.aggregate([{ $project: { result: { $or: [false, true] } } }]).toArray());

    // OPERATORS - Comparison
    await test('$cmp', () => coll.aggregate([{ $project: { result: { $cmp: ['$age', 30] } } }]).toArray());
    await test('$eq', () => coll.aggregate([{ $project: { result: { $eq: ['$age', 30] } } }]).toArray());
    await test('$gt', () => coll.aggregate([{ $project: { result: { $gt: ['$age', 25] } } }]).toArray());
    await test('$gte', () => coll.aggregate([{ $project: { result: { $gte: ['$age', 30] } } }]).toArray());
    await test('$lt', () => coll.aggregate([{ $project: { result: { $lt: ['$age', 30] } } }]).toArray());
    await test('$lte', () => coll.aggregate([{ $project: { result: { $lte: ['$age', 30] } } }]).toArray());
    await test('$ne', () => coll.aggregate([{ $project: { result: { $ne: ['$age', 30] } } }]).toArray());

    // OPERATORS - Conditional
    await test('$cond', () => coll.aggregate([{ $project: { result: { $cond: [{ $gt: ['$age', 30] }, 'old', 'young'] } } }]).toArray());
    await test('$ifNull', () => coll.aggregate([{ $project: { result: { $ifNull: ['$missing', 'default'] } } }]).toArray());
    await test('$switch', () => coll.aggregate([{ $project: { result: { $switch: { branches: [{ case: { $eq: ['$age', 25] }, then: 'match' }], default: 'no match' } } } }]).toArray());

    // OPERATORS - Date
    await test('$dateAdd', () => coll.aggregate([{ $project: { result: { $dateAdd: { startDate: '$date', unit: 'day', amount: 1 } } } }]).toArray());
    await test('$dateDiff', () => coll.aggregate([{ $project: { result: { $dateDiff: { startDate: '$date', endDate: new Date(), unit: 'day' } } } }]).toArray());
    await test('$dateFromParts', () => coll.aggregate([{ $project: { result: { $dateFromParts: { year: 2024, month: 1, day: 1 } } } }]).toArray());
    await test('$dateFromString', () => coll.aggregate([{ $project: { result: { $dateFromString: { dateString: '2024-01-01' } } } }]).toArray());
    await test('$dateSubtract', () => coll.aggregate([{ $project: { result: { $dateSubtract: { startDate: '$date', unit: 'day', amount: 1 } } } }]).toArray());
    await test('$dateToParts', () => coll.aggregate([{ $project: { result: { $dateToParts: { date: '$date' } } } }]).toArray());
    await test('$dateToString', () => coll.aggregate([{ $project: { result: { $dateToString: { date: '$date', format: '%Y-%m-%d' } } } }]).toArray());
    await test('$dateTrunc', () => coll.aggregate([{ $project: { result: { $dateTrunc: { date: '$date', unit: 'month' } } } }]).toArray());
    await test('$dayOfMonth', () => coll.aggregate([{ $project: { result: { $dayOfMonth: '$date' } } }]).toArray());
    await test('$dayOfWeek', () => coll.aggregate([{ $project: { result: { $dayOfWeek: '$date' } } }]).toArray());
    await test('$dayOfYear', () => coll.aggregate([{ $project: { result: { $dayOfYear: '$date' } } }]).toArray());
    await test('$hour', () => coll.aggregate([{ $project: { result: { $hour: '$date' } } }]).toArray());
    await test('$isoDayOfWeek', () => coll.aggregate([{ $project: { result: { $isoDayOfWeek: '$date' } } }]).toArray());
    await test('$isoWeek', () => coll.aggregate([{ $project: { result: { $isoWeek: '$date' } } }]).toArray());
    await test('$isoWeekYear', () => coll.aggregate([{ $project: { result: { $isoWeekYear: '$date' } } }]).toArray());
    await test('$millisecond', () => coll.aggregate([{ $project: { result: { $millisecond: '$date' } } }]).toArray());
    await test('$minute', () => coll.aggregate([{ $project: { result: { $minute: '$date' } } }]).toArray());
    await test('$month', () => coll.aggregate([{ $project: { result: { $month: '$date' } } }]).toArray());
    await test('$second', () => coll.aggregate([{ $project: { result: { $second: '$date' } } }]).toArray());
    await test('$toDate', () => coll.aggregate([{ $project: { result: { $toDate: '$date' } } }]).toArray());
    await test('$tsIncrement', () => coll.aggregate([{ $project: { result: { $tsIncrement: '$ts' } } }]).toArray());
    await test('$tsSecond', () => coll.aggregate([{ $project: { result: { $tsSecond: '$ts' } } }]).toArray());
    await test('$week', () => coll.aggregate([{ $project: { result: { $week: '$date' } } }]).toArray());
    await test('$year', () => coll.aggregate([{ $project: { result: { $year: '$date' } } }]).toArray());

    // OPERATORS - Data Size
    await test('$binarySize', () => coll.aggregate([{ $project: { result: { $binarySize: '$name' } } }]).toArray());
    await test('$bsonSize', () => coll.aggregate([{ $project: { result: { $bsonSize: '$$ROOT' } } }]).toArray());

    // OPERATORS - String
    await test('$concat', () => coll.aggregate([{ $project: { result: { $concat: ['$name', ' test'] } } }]).toArray());
    await test('$dateFromString', () => coll.aggregate([{ $project: { result: { $dateFromString: { dateString: '2024-01-01' } } } }]).toArray());
    await test('$indexOfBytes', () => coll.aggregate([{ $project: { result: { $indexOfBytes: ['$name', 'l'] } } }]).toArray());
    await test('$indexOfCP', () => coll.aggregate([{ $project: { result: { $indexOfCP: ['$name', 'l'] } } }]).toArray());
    await test('$ltrim', () => coll.aggregate([{ $project: { result: { $ltrim: { input: '  test  ' } } } }]).toArray());
    await test('$regexFind', () => coll.aggregate([{ $project: { result: { $regexFind: { input: '$name', regex: /^A/ } } } }]).toArray());
    await test('$regexFindAll', () => coll.aggregate([{ $project: { result: { $regexFindAll: { input: '$name', regex: /[aeiou]/i } } } }]).toArray());
    await test('$regexMatch', () => coll.aggregate([{ $project: { result: { $regexMatch: { input: '$name', regex: /^A/ } } } }]).toArray());
    await test('$replaceAll', () => coll.aggregate([{ $project: { result: { $replaceAll: { input: '$name', find: 'a', replacement: 'X' } } } }]).toArray());
    await test('$replaceOne', () => coll.aggregate([{ $project: { result: { $replaceOne: { input: '$name', find: 'a', replacement: 'X' } } } }]).toArray());
    await test('$rtrim', () => coll.aggregate([{ $project: { result: { $rtrim: { input: '  test  ' } } } }]).toArray());
    await test('$split', () => coll.aggregate([{ $project: { result: { $split: ['$name', 'l'] } } }]).toArray());
    await test('$strLenBytes', () => coll.aggregate([{ $project: { result: { $strLenBytes: '$name' } } }]).toArray());
    await test('$strLenCP', () => coll.aggregate([{ $project: { result: { $strLenCP: '$name' } } }]).toArray());
    await test('$strcasecmp', () => coll.aggregate([{ $project: { result: { $strcasecmp: ['$name', 'alice'] } } }]).toArray());
    await test('$substr', () => coll.aggregate([{ $project: { result: { $substr: ['$name', 0, 3] } } }]).toArray());
    await test('$substrBytes', () => coll.aggregate([{ $project: { result: { $substrBytes: ['$name', 0, 3] } } }]).toArray());
    await test('$substrCP', () => coll.aggregate([{ $project: { result: { $substrCP: ['$name', 0, 3] } } }]).toArray());
    await test('$toLower', () => coll.aggregate([{ $project: { result: { $toLower: '$name' } } }]).toArray());
    await test('$toString', () => coll.aggregate([{ $project: { result: { $toString: '$age' } } }]).toArray());
    await test('$trim', () => coll.aggregate([{ $project: { result: { $trim: { input: '  test  ' } } } }]).toArray());
    await test('$toUpper', () => coll.aggregate([{ $project: { result: { $toUpper: '$name' } } }]).toArray());

    // OPERATORS - Type
    await test('$convert', () => coll.aggregate([{ $project: { result: { $convert: { input: '$age', to: 'string' } } } }]).toArray());
    await test('$isNumber', () => coll.aggregate([{ $project: { result: { $isNumber: '$age' } } }]).toArray());
    await test('$toBool', () => coll.aggregate([{ $project: { result: { $toBool: 1 } } }]).toArray());
    await test('$toDecimal', () => coll.aggregate([{ $project: { result: { $toDecimal: '$age' } } }]).toArray());
    await test('$toDouble', () => coll.aggregate([{ $project: { result: { $toDouble: '$age' } } }]).toArray());
    await test('$toInt', () => coll.aggregate([{ $project: { result: { $toInt: '$score' } } }]).toArray());
    await test('$toLong', () => coll.aggregate([{ $project: { result: { $toLong: '$age' } } }]).toArray());
    await test('$toObjectId', () => coll.aggregate([{ $project: { result: { $toObjectId: '507f1f77bcf86cd799439011' } } }]).toArray());
    await test('$type', () => coll.aggregate([{ $project: { result: { $type: '$age' } } }]).toArray());
    await test('$toHashedIndexKey', () => coll.aggregate([{ $project: { result: { $toHashedIndexKey: '$name' } } }]).toArray());
    await test('$toUUID', () => coll.aggregate([{ $project: { result: { $toUUID: '12345678-1234-1234-1234-123456789012' } } }]).toArray());

    // OPERATORS - Accumulator (in $group)
    await test('$accumulator', () => coll.aggregate([{ $group: { _id: null, result: { $accumulator: { init: 'function() { return 0; }', accumulate: 'function(state, val) { return state + val; }', accumulateArgs: ['$age'], merge: 'function(s1, s2) { return s1 + s2; }', lang: 'js' } } } }]).toArray());
    await test('$addToSetGroup', () => coll.aggregate([{ $group: { _id: null, result: { $addToSet: '$dept' } } }]).toArray());
    await test('$avgInGroup', () => coll.aggregate([{ $group: { _id: null, result: { $avg: '$age' } } }]).toArray());
    await test('$avgInProject', () => coll.aggregate([{ $project: { result: { $avg: '$items.qty' } } }]).toArray());
    await test('$bottom', () => coll.aggregate([{ $group: { _id: null, result: { $bottom: { output: '$name', sortBy: { age: 1 } } } } }]).toArray());
    await test('$maxInGroup', () => coll.aggregate([{ $group: { _id: null, result: { $max: '$age' } } }]).toArray());
    await test('$maxInProject', () => coll.aggregate([{ $project: { result: { $max: ['$age', '$score'] } } }]).toArray());
    await test('$mergeObjectsInGroup', () => coll.aggregate([{ $group: { _id: null, result: { $mergeObjects: { name: '$name' } } } }]).toArray());
    await test('$mergeObjectsInProject', () => coll.aggregate([{ $project: { result: { $mergeObjects: [{ a: 1 }, { b: 2 }] } } }]).toArray());
    await test('$minInGroup', () => coll.aggregate([{ $group: { _id: null, result: { $min: '$age' } } }]).toArray());
    await test('$minInProject', () => coll.aggregate([{ $project: { result: { $min: ['$age', '$score'] } } }]).toArray());
    await test('$bottomN', () => coll.aggregate([{ $group: { _id: null, result: { $bottomN: { output: '$name', sortBy: { age: 1 }, n: 2 } } } }]).toArray());
    await test('$count', () => coll.aggregate([{ $group: { _id: null, result: { $count: {} } } }]).toArray());
    await test('$firstInGroup', () => coll.aggregate([{ $sort: { age: 1 } }, { $group: { _id: null, result: { $first: '$name' } } }]).toArray());
    await test('$firstNInGroup', () => coll.aggregate([{ $group: { _id: null, result: { $firstN: { input: '$name', n: 2 } } } }]).toArray());
    await test('$lastInGroup', () => coll.aggregate([{ $sort: { age: 1 } }, { $group: { _id: null, result: { $last: '$name' } } }]).toArray());
    await test('$lastNInGroup', () => coll.aggregate([{ $group: { _id: null, result: { $lastN: { input: '$name', n: 2 } } } }]).toArray());
    await test('$maxNInGroup', () => coll.aggregate([{ $group: { _id: null, result: { $maxN: { input: '$age', n: 2 } } } }]).toArray());
    await test('$medianInGroup', () => coll.aggregate([{ $group: { _id: null, result: { $median: { input: '$age', method: 'approximate' } } } }]).toArray());
    await test('$minNInGroup', () => coll.aggregate([{ $group: { _id: null, result: { $minN: { input: '$age', n: 2 } } } }]).toArray());
    await test('$percentile', () => coll.aggregate([{ $group: { _id: null, result: { $percentile: { input: '$age', p: [0.5, 0.75], method: 'approximate' } } } }]).toArray());
    await test('$percentileInProject', () => coll.aggregate([{ $project: { result: { $percentile: { input: ['$age', '$score'], p: [0.5], method: 'approximate' } } }}]).toArray());
    await test('$push', () => coll.aggregate([{ $group: { _id: null, result: { $push: '$name' } } }]).toArray());
    await test('$stdDevPop', () => coll.aggregate([{ $group: { _id: null, result: { $stdDevPop: '$age' } } }]).toArray());
    await test('$stdDevSamp', () => coll.aggregate([{ $group: { _id: null, result: { $stdDevSamp: '$age' } } }]).toArray());
    await test('$sum', () => coll.aggregate([{ $group: { _id: null, result: { $sum: '$age' } } }]).toArray());
    await test('$sumInProject', () => coll.aggregate([{ $project: { result: { $sum: ['$age', '$score'] } } }]).toArray());
    await test('$stdDevPopInProject', () => coll.aggregate([{ $project: { result: { $stdDevPop: ['$age', '$score'] } } }]).toArray());
    await test('$stdDevSampInProject', () => coll.aggregate([{ $project: { result: { $stdDevSamp: ['$age', '$score'] } } }]).toArray());
    await test('$top', () => coll.aggregate([{ $group: { _id: null, result: { $top: { output: '$name', sortBy: { age: -1 } } } } }]).toArray());
    await test('$topN', () => coll.aggregate([{ $group: { _id: null, result: { $topN: { output: '$name', sortBy: { age: -1 }, n: 2 } } } }]).toArray());

    // OPERATORS - Window Functions (in $setWindowFields)
    await test('$addToSetWindow', () => coll.aggregate([{$setWindowFields: {sortBy: { age: 1 },output: {testList: {$addToSet: "$dept",window: {documents: ["unbounded", "current"]}}}}}]).toArray())
    await test('$avgWindow', () => coll.aggregate([{ $setWindowFields: { sortBy: { age: 1 }, output: { result: { $avg: '$score' } } } }]).toArray());
    await test('$bottomWindow', () => coll.aggregate([{ $setWindowFields: { sortBy: { age: 1 }, output: { result: { $bottom: { output: ['$name','$score'], sortBy: {age:1} } } }}}]).toArray());
    await test('$bottomNWindow', () => coll.aggregate([{ $setWindowFields: { sortBy: { age: 1 }, output: { result: { $bottomN: { output: ['$name','$score'], sortBy: { age:1 }, n: 2 } } }}}]).toArray());
    await test('$countWindow', () => coll.aggregate([{ $setWindowFields: { sortBy: { age: 1 }, output: { result: { $count: {} } } } }]).toArray());
    await test('$covariancePop', () => coll.aggregate([{ $setWindowFields: { sortBy: { age: 1 }, output: { result: { $covariancePop: ['$age', '$score'] } } } }]).toArray());
    await test('$covarianceSamp', () => coll.aggregate([{ $setWindowFields: { sortBy: { age: 1 }, output: { result: { $covarianceSamp: ['$age', '$score'] } } } }]).toArray());
    await test('$denseRank', () => coll.aggregate([{ $setWindowFields: { sortBy: { age: 1 }, output: { result: { $denseRank: {} } } } }]).toArray());
    await test('$derivative', () => coll.aggregate([{$setWindowFields: {sortBy: { age: 1 },output: {salaryChange: {$derivative: {input: "$salary"}, window: {documents: ["unbounded", "unbounded"]}}}}}]).toArray())
    await test('$documentNumber', () => coll.aggregate([{ $setWindowFields: { sortBy: { age: 1 }, output: { result: { $documentNumber: {} } } } }]).toArray());
    await test('$expMovingAvg', () => coll.aggregate([{ $setWindowFields: { sortBy: { age: 1 }, output: { result: { $expMovingAvg: { input: '$score', N: 2 } } } } }]).toArray());
    await test('$firstWindow', () => coll.aggregate([{ $setWindowFields: { sortBy: { age: 1 }, output: { result: { $first: '$name' } } } }]).toArray());
    await test('$firstNWindow', () => coll.aggregate([{ $setWindowFields: { sortBy: { age: 1 }, output: { result: { $firstN: { input: '$name', n: 2 } } } } }]).toArray());
    await test('$integral', () => coll.aggregate([{ $setWindowFields: { sortBy: { age: 1 }, output: { result: { $integral: { input: '$score' } } } } }]).toArray());
    await test('$lastWindow', () => coll.aggregate([{ $setWindowFields: { sortBy: { age: 1 }, output: { result: { $last: '$name' } } } }]).toArray());
    await test('$lastNWindow', () => coll.aggregate([{ $setWindowFields: { sortBy: { age: 1 }, output: { result: { $lastN: { input: '$name', n: 2 } } } } }]).toArray());
    await test('$locf', () => coll.aggregate([{ $setWindowFields: { sortBy: { age: 1 }, output: { result: { $locf: '$score' } } } }]).toArray());
    await test('$maxWindow', () => coll.aggregate([{ $setWindowFields: { sortBy: { age: 1 }, output: { result: { $max: '$score' } } } }]).toArray());
    await test('$maxNWindow', () => coll.aggregate([{ $setWindowFields: { sortBy: { age: 1 }, output: { result: { $maxN: { input: '$score', n: 2 } } } } }]).toArray());
    await test('$medianWindow', () => coll.aggregate([{ $setWindowFields: { sortBy: { age: 1 }, output: { result: { $median: { input: '$score', method: 'approximate' } } } } }]).toArray());
    await test('$minWindow', () => coll.aggregate([{ $setWindowFields: { sortBy: { age: 1 }, output: { result: { $min: '$score' } } } }]).toArray());
    await test('$minNWindow', () => coll.aggregate([{ $setWindowFields: { sortBy: { age: 1 }, output: { result: { $minN: { input: '$score', n: 2 } } } } }]).toArray());
    await test('$percentileWindow', () => coll.aggregate([{ $setWindowFields: { sortBy: { age: 1 }, output: { result: { $percentile: { input: '$score', p: [0.5], method: 'approximate' } } } } }]).toArray());
    await test('$pushWindow', () => coll.aggregate([{ $setWindowFields: { sortBy: { age: 1 }, output: { result: { $push: '$name' } } } }]).toArray());
    await test('$shift', () => coll.aggregate([{ $setWindowFields: { sortBy: { age: 1 }, output: { result: { $shift: { output: '$name', by: 1 } } } } }]).toArray());
    await test('$stdDevPopWindow', () => coll.aggregate([{ $setWindowFields: { sortBy: { age: 1 }, output: { result: { $stdDevPop: '$score' } } } }]).toArray());
    await test('$stdDevSampWindow', () => coll.aggregate([{ $setWindowFields: { sortBy: { age: 1 }, output: { result: { $stdDevSamp: '$score' } } } }]).toArray());
    await test('$sumWindow', () => coll.aggregate([{ $setWindowFields: { sortBy: { age: 1 }, output: { result: { $sum: '$score' } } } }]).toArray());
    await test('$topWindow', () => coll.aggregate([{ $setWindowFields: { sortBy: { age: 1 }, output: { result: { $top: { output: '$name', sortBy: { score: -1 } } } } } }]).toArray());
    await test('$topNWindow', () => coll.aggregate([{ $setWindowFields: { sortBy: { age: 1 }, output: { result: { $topN: { output: '$name', sortBy: { score: -1 }, n: 2 } } } } }]).toArray());

    // OPERATORS - Variable
    await test('$let', () => coll.aggregate([{ $project: { result: { $let: { vars: { x: 5 }, in: { $multiply: ['$$x', 2] } } } } }]).toArray());

    // OPERATORS - Query and Projection (in $match)
    await test('$all', () => coll.aggregate([{ $match: { tags: { $all: ['a', 'b'] } } }]).toArray());
    await test('$alwaysFalse', () => coll.aggregate([{ $match: { $alwaysFalse: 1 } }]).toArray());
    await test('$alwaysTrue', () => coll.aggregate([{ $match: { $alwaysTrue: 1 } }]).toArray());
    await test('$andQuery', () => coll.aggregate([{ $match: { $and: [{ age: { $gt: 25 } }, { score: { $gt: 80 } }] } }]).toArray());
    await test('$bitsAllClear', () => coll.aggregate([{ $match: { age: { $bitsAllClear: [1, 5] } } }]).toArray());
    await test('$bitsAllSet', () => coll.aggregate([{ $match: { age: { $bitsAllSet: [1, 5] } } }]).toArray());
    await test('$bitsAnyClear', () => coll.aggregate([{ $match: { age: { $bitsAnyClear: [1, 5] } } }]).toArray());
    await test('$bitsAnySet', () => coll.aggregate([{ $match: { age: { $bitsAnySet: [1, 5] } } }]).toArray());
    await test('$comment', () => coll.aggregate([{ $match: { age: { $gt: 25 }, $comment: 'test comment' } }]).toArray());
    await test('$elemMatch', () => coll.aggregate([{ $match: { items: { $elemMatch: { qty: { $gt: 4 } } } } }]).toArray());
    await test('$exists', () => coll.aggregate([{ $match: { name: { $exists: true } } }]).toArray());
    await test('$exprMatch', () => coll.aggregate([{ $match: { $expr: { $gt: ['$age', 25] } } }]).toArray());
    await test('$geoIntersectsFind', () => coll.find({location: {$geoIntersects: {$geometry: {type : "Polygon" ,coordinates: [ [ [ -73, 0 ], [ -73, 40 ], [ 0, 40 ], [ 0, 0 ], [ -73, 0 ] ] ]}}}}).toArray());
    await test('$geoIntersectsMatch', () => coll.aggregate([{$match: {location: {$geoIntersects: {$geometry: {type : "Polygon" ,coordinates: [ [ [ -73, 0 ], [ -73, 40 ], [ 0, 40 ], [ 0, 0 ], [ -73, 0 ] ] ]}}}}}]).toArray());
    await test('$geoWithinFind', () => coll.find({location: {$geoWithin: {$geometry: {type : "Polygon" ,coordinates: [ [ [ -73, 0 ], [ -73, 40 ], [ 0, 40 ], [ 0, 0 ], [ -73, 0 ] ] ]}}}}).toArray());
    await test('$geoWithinMatch', () => coll.aggregate([{$match: {location: {$geoWithin: {$geometry: {type : "Polygon" ,coordinates: [ [ [ -73, 0 ], [ -73, 40 ], [ 0, 40 ], [ 0, 0 ], [ -73, 0 ] ] ]}}}}}]).toArray());
    await test('$gteQuery', () => coll.aggregate([{ $match: { age: { $gte: 30 } } }]).toArray());
    await test('$inQuery', () => coll.aggregate([{ $match: { dept: { $in: ['Sales', 'IT'] } } }]).toArray());
    await test('$jsonSchema', () => coll.aggregate([{ $match: { $jsonSchema: { required: ['name'] } } }]).toArray());
    await test('$lteQuery', () => coll.aggregate([{ $match: { age: { $lte: 30 } } }]).toArray());
    await test('$modQuery', () => coll.aggregate([{ $match: { age: { $mod: [5, 0] } } }]).toArray());
    await test('$nearSphere', () => coll.find({location: { $nearSphere: { $geometry: { type: "Point", coordinates: [-73.99279, 40.719296] }, $maxDistance: 2000 }}}).toArray());
    await test('$nin', () => coll.aggregate([{ $match: { dept: { $nin: ['HR', 'Finance'] } } }]).toArray());
    await test('$norQuery', () => coll.aggregate([{ $match: { $nor: [{ age: { $lt: 25 } }, { score: { $lt: 80 } }] } }]).toArray());
    await test('$notQuery', () => coll.aggregate([{ $match: { age: { $not: { $lt: 30 } } } }]).toArray());
    await test('$orQuery', () => coll.aggregate([{ $match: { $or: [{ age: { $lt: 26 } }, { age: { $gt: 33 } }] } }]).toArray());
    await test('$regexQuery', () => coll.aggregate([{ $match: { name: { $regex: /^A/ } } }]).toArray());
    await test('$sampleRate', () => coll.aggregate([{ $match: { $sampleRate: 0.5 } }]).toArray());
    await test('$sizeQuery', () => coll.aggregate([{ $match: { tags: { $size: 2 } } }]).toArray());
    await test('$text', () => Promise.resolve([]), true);
    await test('$typeQuery', () => coll.aggregate([{ $match: { name: { $type: 'string' } } }]).toArray());

    // OPERATORS - Object
    await test('$setField', () => coll.aggregate([{ $project: { result: { $setField: { field: 'newField', input: '$$ROOT', value: 'newValue' } } } }]).toArray());
    await test('$unsetField', () => coll.aggregate([{ $project: { result: { $unsetField: { field: 'age', input: '$$ROOT' } } } }]).toArray());

    // OPERATORS - Miscellaneous
    await test('$const', () => coll.aggregate([{ $project: { result: { $const: 'constant' } } }]).toArray());
    await test('$function', () => coll.aggregate([{ $project: { result: { $function: { body: 'function(x) { return x * 2; }', args: ['$age'], lang: 'js' } } } }]).toArray());
    await test('$getField', () => coll.aggregate([{ $project: { result: { $getField: 'name' } } }]).toArray());
    await test('$literal', () => coll.aggregate([{ $project: { result: { $literal: '$name' } } }]).toArray());
    await test('$meta', () => coll.aggregate([{ $project: { result: { $meta: 'searchScore' } } }]).toArray());
    await test('$rand', () => coll.aggregate([{ $project: { result: { $rand: {} } } }]).toArray());
    await test('$exprFind', () => coll.find({ $expr: { $gt: [ '$age', 30 ] } }).toArray());
    await test('$nearFind', () => coll.find({location: {$near: {$geometry: {type : "Point" ,coordinates: [ -73, 35 ]}, $maxDistance: 1000000}}}).toArray())
    await test('$where', () => coll.find({ $where: 'this.score > this.age' } ).toArray());

    // UPDATE OPERATORS
    console.log(`** UPDATE OPERATIONS`);
    await test('$setUpdate', async () => { await coll.updateOne({ _id: 1 }, { $set: { updated: true } }); return []; });
    await test('$unsetUpdate', async () => { await coll.updateOne({ _id: 1 }, { $unset: { updated: '' } }); return []; });
    await test('$inc', async () => { await coll.updateOne({ _id: 1 }, { $inc: { age: 1 } }); return []; });
    await test('$mul', async () => { await coll.updateOne({ _id: 1 }, { $mul: { score: 1.1 } }); return []; });
    await test('$rename', async () => { await coll.updateOne({ _id: 1 }, { $rename: { score: 'points' } }); return []; });
    await test('$minUpdate', async () => { await coll.updateOne({ _id: 2 }, { $min: { age: 20 } }); return []; });
    await test('$maxUpdate', async () => { await coll.updateOne({ _id: 2 }, { $max: { age: 40 } }); return []; });
    await test('$currentDate', async () => { await coll.updateOne({ _id: 2 }, { $currentDate: { lastModified: true } }); return []; });
    await test('$addToSetUpdate', async () => { await coll.updateOne({ _id: 2 }, { $addToSet: { tags: 'e' } }); return []; });
    await test('$pop', async () => { await coll.updateOne({ _id: 2 }, { $pop: { tags: 1 } }); return []; });
    await test('$pull', async () => { await coll.updateOne({ _id: 3 }, { $pull: { tags: 'a' } }); return []; });
    await test('$pushUpdate', async () => { await coll.updateOne({ _id: 3 }, { $push: { tags: 'x' } }); return []; });
    await test('$pullAll', async () => { await coll.updateOne({ _id: 3 }, { $pullAll: { tags: ['x', 'c'] } }); return []; });
    await test('$each', async () => { await coll.updateOne({ _id: 4 }, { $push: { tags: { $each: ['y', 'z'] } } }); return []; });
    await test('$position', async () => { await coll.updateOne({ _id: 4 }, { $push: { tags: { $each: ['w'], $position: 0 } } }); return []; });
    await test('$slice', async () => { await coll.updateOne({ _id: 4 }, { $push: { tags: { $each: [], $slice: 2 } } }); return []; });
    await test('$sort', async () => { await coll.updateOne({ _id: 4 }, { $push: { tags: { $each: [], $sort: 1 } } }); return []; });
    await test('$bit', async () => { await coll.updateOne({ _id: 1 }, { $bit: { age: { and: 30 } } }); return []; });
    await test('$setOnInsert', async () => { await coll.updateOne({ _id: 10 }, { $setOnInsert: { created: new Date() } }, { upsert: true }); return []; });

  } catch (err) {
    console.error('Setup error:', err.message);
  } finally {
    await client.close();
    printResults();
  }

  async function test(name, fn, skip = false) {
    if (skip) {
      console.log(`⊘ ${name} - SKIPPED`);
      return;
    }
    try {
      const result = await fn();
      results.passed.push(name);
      console.log(`✓ ${name}`);
    } catch (err) {
      results.failed.push({ name, error: err.message });
      console.log(`✗ ${name} - ${err.message}`);
    }
  }

  function printResults() {
    console.log('\n' + '='.repeat(50));
    console.log(`PASSED: ${results.passed.length}`);
    console.log(`FAILED: ${results.failed.length}`);
    console.log('='.repeat(50));
    if (results.failed.length > 0) {
      console.log('\nFailed tests:');
      results.failed.forEach(f => console.log(`  - ${f.name}: ${f.error}`));
    }
  }
}

runTests();
