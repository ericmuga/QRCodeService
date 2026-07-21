<?php

namespace App\Http\Controllers;

use App\Models\Helpers;
use Carbon\Carbon;
use Illuminate\Http\Request;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log;
use GuzzleHttp\Promise;
use Illuminate\Support\Facades\Http;

class ApiServiceController extends Controller
{
    public function getPortalOrdersApi()
    {
        $url = config('app.portal_orders_url'); // Replace with the actual API endpoint

        $ch = curl_init($url);

        // Set cURL options
        curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
        curl_setopt($ch, CURLOPT_HEADER, false);

        // Execute cURL request
        $response = curl_exec($ch);

        // Check for cURL errors
        if (curl_errno($ch)) {
            // Handle error
            $error = curl_error($ch);
            // Handle or log the error appropriately
        }

        // Close cURL session
        curl_close($ch);

        $action = false;

        // Process the API response
        if (!empty($response)) {
            $data = json_decode($response, true);
            // Process and use $data as needed
            $action = $this->insertPortalOrders($data);
        }

        // Return the response
        $res = ['success' => $action, 'action' => 'Orders Insert', 'timestamp' => now()->addHours(3)];
        return response()->json($res);
    }

    public function insertPortalOrders($data)
    {
        try {
            // Get today's date and calculate tomorrow's date
            $tomorrow = now()->addDay()->format('Y-m-d');

            // Filter data to include only records with shipment_date >= tomorrow
            $filteredData = array_filter($data, function ($d) {
                return $d['shipment_date'] !== '01/08/2025';
            });

            // Prepare all rows first so chunk size is based on actual column count.
            $upsertRows = array_map(function ($d) {
                return [
                    'External Document No_' => $d['tracking_no'],
                    'Line No_' => $d['id'],
                    'Sell-to Customer No_' => $d['customer_code'],
                    'Shipment Date' => $d['shipment_date'],
                    'Salesperson Code' => $d['sales_code'],
                    'Ship-to Code' => $d['ship_to_code'],
                    'Ship-to Name' => $d['ship_to_name'],
                    'Item No_' => $d['item_code'],
                    'Quantity' => $d['quantity'],
                    'Unit of Measure' => $d['unit_of_measure'],
                    'Status' => 0,
                    'Product Specification' => $d['product_specifications'],
                    'Customer Specification' => $d['customer_specification'],
                    'Expected Line Count' => $d['expected_line_count'],
                    'Error Message' => '',
                    'PDA Order' => 0,
                    'Company' => 'FCL',
                ];
            }, $filteredData);

            if (empty($upsertRows)) {
                info('Portal Orders Inserted/Updated: []');
                return true;
            }

            // SQL Server allows max 2100 bound params. Keep a small buffer to avoid edge-case overflows.
            $maxSqlParams = 2100;
            $safetyBuffer = 50;
            $columnsPerRecord = count($upsertRows[0]);
            $maxBatchSize = max(1, (int) floor(($maxSqlParams - $safetyBuffer) / $columnsPerRecord));

            // Chunk the upsert payload using the computed safe batch size.
            $dataChunks = array_chunk($upsertRows, $maxBatchSize);

            foreach ($dataChunks as $chunk) {
                // Perform upsert
                DB::connection('bc240')
                    ->table('FCL1$Imported Orders$23dc970e-11e8-4d9b-8613-b7582aec86ba')
                    ->upsert(
                        $chunk,
                        ['External Document No_', 'Line No_'], // Unique constraints for conflict resolution
                        [
                            'Sell-to Customer No_', 'Shipment Date', 'Salesperson Code', 
                            'Ship-to Code', 'Ship-to Name', 'Item No_', 
                            'Quantity', 'Unit of Measure', 'Status', 'Customer Specification'
                        ] // Columns to update on conflict
                    );

                // help test connection into orders
                // try {
                //     // Attempt to fetch a single record from the orders connection to test connectivity
                //     $testOrder = DB::connection('orders')
                //         ->table('bot_orders')
                //         ->limit(1)
                //         ->first();

                //     if ($testOrder) {
                //         Log::info('Successfully connected to orders database and fetched a record from bot_orders.');
                //     } else {
                //         Log::warning('Connected to orders database but no records found in bot_orders.');
                //     }
                // } catch (\Exception $ex) {
                //     Log::error('Failed to connect to orders database: ' . $ex->getMessage());
                // }
            }

            info('Portal Orders Inserted/Updated: ' . json_encode($filteredData));
            return true;

        } catch (\Exception $e) {
            Log::error('Exception in ' . __METHOD__ . '(): ' . $e->getMessage());
            return false;
        }
    }

    public function getVendorList($from = null, $to = null)
    {
        $results = DB::connection('bc240')->table('FCL1$Vendor$437dbf0e-84ff-417a-965d-ed2bb9650972 as a')
            ->join('FCL1$SlaughterData$23dc970e-11e8-4d9b-8613-b7582aec86ba as b', 'a.No_', '=', 'b.VendorNo')
            ->select(
                'a.No_',
                'a.Phone No_ as Phone',
                'a.Contact',
                'a.Name',
                'a.City',
                'b.Settlement Date AS SettlementDate',
            )
            ->where('a.Vendor Posting Group', 'PIGFARMERS')
            ->where('a.Blocked', 0)
            ->when($from && $to, function ($query) use ($from, $to) {
                return $query->whereDate('b.Settlement Date', '>=', $from)
                    ->whereDate('b.Settlement Date', '<=', $to);
            }, function ($query) {
                return $query->whereDate('b.Settlement Date', '>=', now()->subMonth());
            })
            ->orderBy('b.Settlement Date', 'asc')
            ->distinct('a.No_')
            ->get();

        $insert_res = [true, 'No items'];

        if (!empty($results)) {
            $insert_res = $this->saveVendorsListApi($results);
        }

        $decoded_res = json_decode($insert_res);

        // Return the response
        $res = ['success' => $decoded_res[0], 'Description' => $decoded_res[1], 'action' => 'Vendors List Insert', 'timestamp' => now()->addHours(3)];

        return response()->json($res);
    }

    public function saveVendorsListApi($post_data)
    {
        $url = config('app.save_vendors_list_api');

        $helpers = new Helpers();

        $res = $helpers->send_curl($url, $post_data);

        return $res;
    }

    public function ordersStatusMain()
    {
        $salesHeader = DB::connection('bc240')->table('FCL1$Sales Header$437dbf0e-84ff-417a-965d-ed2bb9650972 as a')
            ->whereIn('a.Document Type', ['2', '1'])
            ->whereDate('a.Posting Date', '>=', today())
            ->whereRaw("CHARINDEX(('-' + a.[Salesperson Code] + '-'), a.[External Document No_]) <> 0")
            ->select(
                'a.External Document No_ AS external_doc_no',
                DB::raw('
                            CASE
                                WHEN (a.[Status] = 4) AND (a.[Document Type] = 1) THEN 3 -- execute
                                WHEN (a.[Document Type] = 2) OR (a.[Status] = 1) THEN 4 -- post
                                WHEN (a.[Status] = 0) THEN 2 -- make order
                                ELSE NULL -- You can replace NULL with a default value if needed
                            END as [Status]')
            )
            ->get();

        $salesInvoiceHeader = DB::connection('bc240')->table('FCL1$Sales Invoice Header$437dbf0e-84ff-417a-965d-ed2bb9650972 as b')
            ->whereDate('b.Posting Date', '>=', today())
            ->whereRaw("CHARINDEX(('-' + b.[Salesperson Code] + '-'), b.[External Document No_]) <> 0")
            ->select(
                'External Document No_ AS external_doc_no',
                DB::raw('4 As Status')
            )
            ->get();

        $imported = DB::connection('bc240')->table('FCL1$Imported Orders$23dc970e-11e8-4d9b-8613-b7582aec86ba')
            ->whereDate('Shipment Date', '>=', today())
            ->whereNotIn('External Document No_', $salesHeader->pluck('external_doc_no'))
            ->whereNotIn('External Document No_', $salesInvoiceHeader->pluck('external_doc_no'))
            ->select(
                'External Document No_ AS external_doc_no',
                DB::raw('2 As Status')
            )
            ->get();

        // Merge the result sets
        $mergedResults = $salesHeader
            ->concat($salesHeader)
            ->concat($salesInvoiceHeader)
            ->concat($imported);

        // return ($mergedResults);

        if (!empty($mergedResults)) {
            $action = $this->updateStatusApi(json_encode($mergedResults));
        }

        return $action;
    }

    public function updateStatusApi($post_data)
    {
        $url = config('app.update_orders_status_url');

        $helpers = new Helpers();

        $res = $helpers->send_curl($url, $post_data);

        return $res;
    }

    public function getEmployeeList()
    {
        $emps = DB::connection('bc240')->table('FCL1$Employee$437dbf0e-84ff-417a-965d-ed2bb9650972 as a')
            ->select(
                'a.No_'
            )
            ->get();

        return response()->json($emps);
    }

    public function fetchSaveShipments()
    {
        $headers = DB::connection('bc240')->table('FCL1$Sales Invoice Header$437dbf0e-84ff-417a-965d-ed2bb9650972 as a')
            // ->join('FCL$Sales Invoice Header$78dbdf4c-61b4-455a-a560-97eaca9a08b7 as b', 'a.No_', '=', 'b.No_')
            ->where('b.ShipmentNo', '!=', '')
            ->whereDate('a.Shipment Date', '>=', today()->subDays(1))
            ->select(
                'a.No_ as shipment_no',
                'a.No_ as invoice_no',
                'a.Salesperson Code as sales_code',
                'a.Sell-to Customer No_ as customer_no',
                'a.Sell-to Customer Name as customer_name',
                'a.Ship-to Code as ship_to_code',
                'a.Ship-to Name as ship_to_name',
                'a.Shipment Date as shipment_date',
                'a.Quote No_ as quote_no',
                'a.Posting Date as posting_date'
            )
            ->orderBy('a.Posting Date', 'asc')
            ->get();

        if (empty($headers)) {
            return response()->json(['success' => true, 'message' => 'No Shipment data to insert.', 'timestamp' => now()->addHours(3)]);
        }

        // request fcl orders system to save data
        $url = config('app.save_shipments');

        $helpers = new Helpers();
        $res = $helpers->send_curl($url, json_encode($headers));

        return response()->json($res);
    }

    public function fetchSaveShipmentLines()
    {
        $lines = DB::connection('bc240')->table('FCL1$Sales Invoice Line$437dbf0e-84ff-417a-965d-ed2bb9650972 as a')
            ->where('a.Document No_', '!=', '')
            ->whereNotNull('a.No_')
            ->where('a.No_', '!=', '')
            ->where('a.No_', '!=', '41990')
            ->whereDate('a.Shipment Date', today())
            ->select(
                'a.Document No_ as shipment_no',
                'a.No_ as item_code',
                'a.Quantity as quantity',
                'a.Unit of Measure Code as unit_measure'
            )
            ->get();

        if (empty($lines)) {
            return response()->json(['success' => true, 'message' => 'No Shipment lines data to insert.', 'timestamp' => now()->addHours(3)]);
        }

        // request fcl orders system to save data
        $url = config('app.save_shipments_lines');

        $helpers = new Helpers();
        $res = $helpers->send_curl($url, json_encode($lines));

        return response()->json($res);
    }

    public function fetchDocwynDataAndSave(Request $request)
    {
        $company = $request->has('company') ? $request->company : 'FCL';
        $receivedDate = $request->filled('received_date')
            ? Carbon::parse($request->input('received_date'))->toDateString()
            : Carbon::today()->toDateString();
        $key = config('app.docwyn_api_key');

        $defaultCustomers = [404, 240, 258, 913,914, 420, 823, 824]; 
        $customersInput = $request->input('customers');
        $customers = $defaultCustomers;

        if (is_array($customersInput) && !empty($customersInput)) {
            $customers = array_values(array_unique(array_map(static function ($c) {
                return (int) $c;
            }, $customersInput)));
        }
        // $customers = [913];

        Log::info('DocWyn run parameters', [
            'company' => $company,
            'received_date' => $receivedDate,
            'customers' => $customers,
        ]);

        foreach ($customers as $customer) {
            try {
                // Fetch all data for the customer
                $responseData = $this->fetchDocwynCustomerData($key, $company, $receivedDate, $customer);

                if (empty($responseData)) {
                    continue;
                }

                $processedItems = []; // Keep track of processed items
                $arrays_to_insert240 = [];
                $droppedDuplicateKeys = [];
                $invalidRowsCount = 0;
                $blockedRowsCount = 0;
                $invalidUomRowsCount = 0;
                $invalidQuantityRowsCount = 0;
                $insertedChunkCount = 0;
                $failedChunkCount = 0;

                $collection = collect($responseData);
                // Log::info("DocWyn Data fetched for customer {$customer}: ", $collection->toArray());

                $sortedData = $collection
                    ->sortBy('item_no')
                    ->sortBy('line_no')
                    ->sortBy('ext_doc_no')
                    ->values();

                $blockedExternalDocNos = [
                    '26022785_07_08_2026',
                    '2032130000266_07_07_',
                    '26012640_07_07_2026',
                    '26018053_07_08_2026',
                    'P042749545_21_07_202',
                    'P042750863_21_07_202'
                ];

                $blockedItemNos = [
                    'J31010203', 'J31010402', 'J31010403', 'J31010404', 'J31010408', 'J31010412', 'J31010414', 'J31010509', 'J31010521', 'J31010604', 'J31010607', 'J31011102', 'J31011103', 'J31011104', 'J31011105', 'J31011401', 'J31011505', 'J31015303', 'J31015306', 'J31015502', 'J31015505', 'J31015602', 'J31015603', 'J31015701', 'J31015822', 'J31015905', 'J31019104', 'J31019109', 'J31020112', 'J31020115', 'J31020401', 'J31020607', 'J31020620', 'J31020621', 'J31020701', 'J31020801', 'J31020812', 'J31021024', 'J31021303', 'J31021305', 'J31021313', 'J31022753', 'J31030101', 'J31030205', 'J31030223', 'J31030224', 'J31030501', 'J31030603', 'J31030604', 'J31030654', 'J31030704', 'J31030805', 'J31030806', 'J31030807', 'J31030809', 'J31030924', 'J31030930', 'J31030943', 'J31030946', 'J31030951', 'J31031007', 'J31031203', 'J31031501', 'J31031507', 'J31031601', 'J31031705', 'J31031708', 'J31031709', 'J31031712', 'J31031720', 'J31031725', 'J31032102', 'J31032104', 'J31040412', 'J31040415', 'J31040503', 'J31040509', 'J31040604', 'J31040803', 'J31040804', 'J31040902', 'J31041001', 'J31041002', 'J31041003', 'J31041004', 'J31041006', 'J31050104', 'J31050203', 'J31050208', 'J31050212', 'J31050216', 'J31050612', 'J31050906', 'J31051250', 'J31060301', 'J31080202', 'J31080203', 'J31080304', 'J31090101', 'J31090119', 'J31090137', 'J31090156', 'J31090159', 'J31090168', 'J31090178', 'J31090190', 'J31090193', 'J31090212', 'J31090214', 'J31090241', 'J31090243', 'J31090257', 'J31090259', 'J31090261', 'J31090273', 'J31090300', 'J31090305', 'J31090325', 'J31090337', 'J31090339', 'J31090345', 'J31090361', 'J31090362', 'J31090363', 'J31090370', 'J31090373', 'J31100102', 'J31100108', 'J31100122', 'J31100123', 'J31100127', 'J31100128', 'J31100129', 'J31100132', 'J31100147', 'J31100157', 'J31100161', 'J31100162', 'J31100171', 'J31100172', 'J31100190', 'J31100198', 'J31100202', 'J31100205', 'J31100217', 'J31100223', 'J31100227', 'J31100228', 'J31100248', 'J31100254', 'J31100257', 'J31100260', 'J31100265', 'J31100270', 'J31100282', 'J31100284', 'J31100285', 'J31100286', 'J31100287', 'J31100291', 'J31100294', 'J31100306', 'J31100307', 'J31100308', 'J31100316', 'J31100324', 'J31100325', 'J31100327', 'J31100330', 'J31100356', 'J31100362', 'J31100364', 'J31100367', 'J31100397', 'J31100398', 'J31100399', 'J31100400', 'J31100403', 'J31100405', 'J31100423', 'J31100503', 'J31120110', 'J31120113', 'J31120114', 'J31120116', 'J31120153', 'J31120161', 'J31121015', 'J31121016', 'J31121020', 'J31121024', 'J31121032', 'J31121035', 'J31121036', 'J31121045', 'J31121048', 'J31121056', 'J31121064', 'J31121068', 'J31150102', 'JX31090122', 'K35010101', 'K35010102', 'K35010108', 'K35010206', 'K35010211', 'K35010216', 'K35010217', 'K35010218', 'K35010220', 'K35010222', 'K35010228', 'K35010233', 'K35010236', 'K35010246', 'K35010249', 'K35010256', 'K35010260', 'K35010264', 'K35010265', 'K35010269', 'K35010406', 'K35010604', 'K35010605', 'K35010701', 'K35010702', 'K35010802', 'K35010803', 'K35010806', 'K35010903', 'K35010905', 'K35010906', 'K35010907', 'K35010908', 'K35010909', 'K35010912', 'K35010913', 'K35010918', 'K35011006', 'K35011104', 'K35011105', 'K35011303', 'K35011304', 'K35011403', 'K35011604', 'K35011605', 'K35011606', 'K35011622', 'K35011623', 'K35011634', 'K35011635', 'K35013006', 'K35013008', 'K35013009', 'K35013010', 'K35013012', 'K35013013', 'K35013014', 'K35013015', 'K35013016', 'K35013017', 'K35013018', 'K35013021', 'K35013023', 'K35013024', 'K35013031', 'K35013032', 'K35013033', 'K35013034', 'K35013036', 'K35013037', 'K35013038', 'K35013039', 'K35013042', 'K35016008', 'K35016011', 'K35016016', 'K35016017', 'K35016030', 'K35016031', 'K35016032', 'K35017002', 'K35017003', 'K35030101', 'K35030106', 'K35030107', 'K35030108', 'K35030109', 'K35030120', 'K35040100', 'K35040107', 'K35040109', 'K35040115', 'K35040116', 'K35040119', 'K35040121', 'K35060103', 'K35060106', 'K35060107', 'K35060109', 'K35060111', 'K35060113', 'K35060115', 'K35060160', 'K35060161', 'K35070103', 'K35070104', 'K35070105', 'K35070109', 'K35070113', 'K35070114', 'K35070115', 'K35070116', 'K35070132', 'K35070134', 'K35070135', 'K35070141', 'K35070142', 'K35070147', 'K35070148', 'K35070149', 'K35070153', 'K35070157', 'K35070158', 'K35070161', 'K35070162', 'K35070164', 'K35070173', 'K35070189', 'K35080120', 'K35080121', 'K35080251', 'K35080273', 'K35080278', 'K35080283', 'K35080295', 'K35090103', 'K35090105', 'K35110203', 'K35120107', 'K35130107', 'K35130110', 'K35130118', 'K35130119', 'K35130121', 'K35130123', 'K35130132', 'K35130137', 'K35130138', 'K35130139', 'K35140105', 'K35160201', 'K35160203', 'K35165177', 'K35165179', 'K35165180', 'K35165182', 'K35165184', 'K35165196', 'K35165220'

                ];

                $allowedUnitOfMeasures = ['KG', 'PC'];

                foreach ($sortedData as $data) {
                    if (!is_array($data) || !array_key_exists('ext_doc_no', $data)) {
                        $invalidRowsCount++;
                        continue;
                    }

                    $rawExternalDocNo = trim((string) $data['ext_doc_no']);
                    $externalDocNo = substr((string) $data['ext_doc_no'], 0, 20);
                    $itemNo = (string) ($data['item_no'] ?? '');

                    if (in_array($externalDocNo, $blockedExternalDocNos, true) || in_array($itemNo, $blockedItemNos, true)) {
                        $blockedRowsCount++;
                        continue;
                    }

                    $uomCode = strtoupper(trim((string) ($data['uom_code'] ?? '')));
                    if (!in_array($uomCode, $allowedUnitOfMeasures, true)) {
                        $invalidUomRowsCount++;
                        continue;
                    }

                    $quantity = abs((float) ($data['quantity'] ?? 0));
                    if ($quantity < 1) {
                        $invalidQuantityRowsCount++;
                        continue;
                    }

                    $lineNo = trim((string) $data['line_no']);

                    // Dedupe should be per original order line using an unambiguous lookup key.
                    $dedupeLookupKey = $rawExternalDocNo . "\x1F" . $lineNo;
                    $uniqueKey = $rawExternalDocNo . '-' . $lineNo;
                    
                    if (isset($processedItems[$dedupeLookupKey])) {
                        $droppedDuplicateKeys[] = $uniqueKey;
                        continue;
                    }

                    $shipmentDate = Carbon::today()->format('Y-m-d H:i:s.000');

                    $arrays_to_insert240[] = [
                        'Company' => $data['company'] ?? 'FCL',
                        'Sell-to Customer No_' => $data['cust_no'],
                        'Customer Specification' => $data['cust_spec'],
                        'Product Specification' => $data['item_spec'],
                        'Expected Line Count' => 0,
                        'Error Message' => '',
                        'PDA Order' => 0,
                        'External Document No_' => $externalDocNo,
                        'Item No_' => $data['item_no'],
                        'Line No_' => $lineNo,
                        'Quantity' => (int) $quantity,
                        'Ship-to Code' => $data['shp_code'],
                        'Shipment Date' => $shipmentDate,
                        'Salesperson Code' => $data['sp_code'],
                        'Unit of Measure' => $uomCode,
                    ];

                    $processedItems[$dedupeLookupKey] = true;
                }

                // Count lines due for insert per external doc from the prepared payload
                // so the value stays tied to attempted rows even if DB upsert later fails.
                $expectedLineCountByDoc = [];
                foreach ($arrays_to_insert240 as $row) {
                    $docNo = (string) $row['External Document No_'];
                    $expectedLineCountByDoc[$docNo] = ($expectedLineCountByDoc[$docNo] ?? 0) + 1;
                }

                foreach ($arrays_to_insert240 as &$row) {
                    $docNo = (string) $row['External Document No_'];
                    $row['Expected Line Count'] = $expectedLineCountByDoc[$docNo] ?? 0;
                }
                unset($row);

                Log::info('Expected line count by document', $expectedLineCountByDoc);

                if (!empty($droppedDuplicateKeys)) {
                    Log::warning("DocWyn duplicate keys dropped for customer {$customer}", [
                        'duplicate_count' => count($droppedDuplicateKeys),
                        'sample_keys' => array_slice($droppedDuplicateKeys, 0, 10),
                    ]);
                }

                Log::info("DocWyn prepared rows for customer {$customer}", [
                    'api_rows' => count($responseData),
                    'prepared_rows' => count($arrays_to_insert240),
                    'invalid_rows' => $invalidRowsCount,
                    'blocked_rows' => $blockedRowsCount,
                    'invalid_uom_rows' => $invalidUomRowsCount,
                    'invalid_quantity_rows' => $invalidQuantityRowsCount,
                    'duplicate_rows' => count($droppedDuplicateKeys),
                ]);

                // Compute a safe chunk size from payload width to stay below SQL Server's 2100 parameter cap.
                $maxSqlParams = 2100;
                $safetyBuffer = 100;
                $columnsPerRecord = !empty($arrays_to_insert240) ? count($arrays_to_insert240[0]) : 1;
                $chunkSize = max(1, (int) floor(($maxSqlParams - $safetyBuffer) / $columnsPerRecord));

                foreach (array_chunk($arrays_to_insert240, $chunkSize) as $chunk) {
                    try {
                        DB::connection('bc240')->table('FCL1$Imported Orders$23dc970e-11e8-4d9b-8613-b7582aec86ba')
                            ->upsert($chunk, ['External Document No_', 'Line No_'], [
                                'Company', 'Sell-to Customer No_', 'Customer Specification', 'Product Specification',
                                'Expected Line Count', 'Error Message', 'PDA Order',
                                'Item No_', 'Quantity', 'Ship-to Code', 'Shipment Date', 'Salesperson Code', 'Unit of Measure'
                            ]);
                        $insertedChunkCount++;
                    } catch (\Exception $e) {
                        $failedChunkCount++;
                        Log::warning("Error inserting data for customer {$customer}: " . $e->getMessage());
                        continue;
                    }
                }

                Log::info("DocWyn insert summary for customer {$customer}", [
                    'chunk_size' => $chunkSize,
                    'successful_chunks' => $insertedChunkCount,
                    'failed_chunks' => $failedChunkCount,
                    'rows_attempted_for_insert' => count($arrays_to_insert240),
                ]);

            } catch (\Exception $e) {
                Log::error('Exception in ' . __METHOD__ . '(): ' . $e->getMessage());
                return response()->json(['error' => $e->getMessage(), 'action' => 'Docwyn fetch & Insert', 'timestamp' => now()->addHours(3)]);
            }
        }

        return response()->json(['message' => 'Data saved successfully', 'action' => 'Docwyn fetch & Insert', 'timestamp' => now()->addHours(3)]);
    }
    
    public function fetchDocwynDataApi(Request $request)
    {
        $company = $request->has('company') ? $request->company : 'FCL';
        $receivedDate = $request->filled('received_date')
            ? Carbon::parse($request->input('received_date'))->toDateString()
            : Carbon::today()->toDateString();
        $key = config('app.docwyn_api_key');

        $defaultCustomers = [404, 240, 258, 913, 914, 420, 823, 824];
        $customersInput = $request->input('customers');
        $customers = $defaultCustomers;

        if (is_array($customersInput) && !empty($customersInput)) {
            $customers = array_values(array_unique(array_map(static function ($c) {
                return (int) $c;
            }, $customersInput)));
        }
        // $customers = [913];

        Log::info('DocWyn run parameters', [
            'company' => $company,
            'received_date' => $receivedDate,
            'customers' => $customers,
        ]);

        $previewRows = [];
        $customerSummaries = [];
        $duplicateSamples = [];

        foreach ($customers as $customer) {
            try {
                // Fetch all data for the customer
                $responseData = $this->fetchDocwynCustomerData($key, $company, $receivedDate, $customer);

                if (empty($responseData)) {
                    $customerSummaries[] = [
                        'customer' => $customer,
                        'api_rows' => 0,
                        'prepared_rows' => 0,
                        'invalid_rows' => 0,
                        'blocked_rows' => 0,
                        'invalid_uom_rows' => 0,
                        'invalid_quantity_rows' => 0,
                        'duplicate_rows' => 0,
                        'duplicate_samples' => [],
                    ];
                    continue;
                }

                $processedItems = []; // Keep track of processed items
                $arrays_to_insert240 = [];
                $droppedDuplicateKeys = [];
                $invalidRowsCount = 0;
                $blockedRowsCount = 0;
                $invalidUomRowsCount = 0;
                $invalidQuantityRowsCount = 0;
                $customerDuplicateSamples = [];

                $collection = collect($responseData);
                // Log::info("DocWyn Data fetched for customer {$customer}: ", $collection->toArray());

                $sortedData = $collection
                    ->sortBy('item_no')
                    ->sortBy('line_no')
                    ->sortBy('ext_doc_no')
                    ->values();

                $blockedExternalDocNos = [
                    '26022785_07_08_2026',
                    '2032130000266_07_07_',
                    '26012640_07_07_2026',
                    '26018053_07_08_2026',
                    'P042749545_21_07_202',
                    'P042750863_21_07_202'
                ];

                $blockedItemNos = [
                    'J31121036',
                    'J31120116',
                    'J31100102',
                    'J31121016',
                    'NOT_FOUND',
                ];

                $allowedUnitOfMeasures = ['KG', 'PC'];

                foreach ($sortedData as $data) {
                    if (!is_array($data) || !array_key_exists('ext_doc_no', $data)) {
                        $invalidRowsCount++;
                        continue;
                    }

                    $rawExternalDocNo = trim((string) $data['ext_doc_no']);
                    $externalDocNo = substr((string) $data['ext_doc_no'], 0, 20);
                    $itemNo = (string) ($data['item_no'] ?? '');

                    if (in_array($externalDocNo, $blockedExternalDocNos, true) || in_array($itemNo, $blockedItemNos, true)) {
                        $blockedRowsCount++;
                        continue;
                    }

                    $uomCode = strtoupper(trim((string) ($data['uom_code'] ?? '')));
                    if (!in_array($uomCode, $allowedUnitOfMeasures, true)) {
                        $invalidUomRowsCount++;
                        continue;
                    }

                    $quantity = abs((float) ($data['quantity'] ?? 0));
                    if ($quantity < 1) {
                        $invalidQuantityRowsCount++;
                        continue;
                    }

                    $lineNo = trim((string) $data['line_no']);

                    // Dedupe should be per original order line using an unambiguous lookup key.
                    $dedupeLookupKey = $rawExternalDocNo . "\x1F" . $lineNo;
                    $uniqueKey = $rawExternalDocNo . '-' . $lineNo;
                    
                    if (isset($processedItems[$dedupeLookupKey])) {
                        $droppedDuplicateKeys[] = $uniqueKey;
                        if (count($customerDuplicateSamples) < 10) {
                            $customerDuplicateSamples[] = [
                                'duplicate_key' => $uniqueKey,
                                'external_doc_no' => $externalDocNo,
                                'line_no' => $lineNo,
                                'item_no' => (string) ($data['item_no'] ?? ''),
                            ];
                        }
                        if (count($duplicateSamples) < 10) {
                            $duplicateSamples[] = [
                                'customer' => $customer,
                                'duplicate_key' => $uniqueKey,
                                'raw_external_doc_no' => $rawExternalDocNo,
                                'external_doc_no' => $externalDocNo,
                                'line_no' => $lineNo,
                                'item_no' => (string) ($data['item_no'] ?? ''),
                            ];
                        }
                        continue;
                    }

                    $shipmentDate = Carbon::today()->format('Y-m-d H:i:s.000');

                    $arrays_to_insert240[] = [
                        'Company' => $data['company'] ?? 'FCL',
                        'Sell-to Customer No_' => $data['cust_no'],
                        'Customer Specification' => $data['cust_spec'],
                        'Product Specification' => $data['item_spec'],
                        'Expected Line Count' => 0,
                        'Error Message' => '',
                        'PDA Order' => 0,
                        'External Document No_' => $externalDocNo,
                        'Item No_' => $data['item_no'],
                        'Line No_' => $lineNo,
                        'Quantity' => (int) $quantity,
                        'Ship-to Code' => $data['shp_code'],
                        'Shipment Date' => $shipmentDate,
                        'Salesperson Code' => $data['sp_code'],
                        'Unit of Measure' => $uomCode,
                    ];

                    $processedItems[$dedupeLookupKey] = true;
                }

                // Count lines due for insert per external doc from the prepared payload
                // so the value stays tied to attempted rows even if DB upsert later fails.
                $expectedLineCountByDoc = [];
                foreach ($arrays_to_insert240 as $row) {
                    $docNo = (string) $row['External Document No_'];
                    $expectedLineCountByDoc[$docNo] = ($expectedLineCountByDoc[$docNo] ?? 0) + 1;
                }

                foreach ($arrays_to_insert240 as &$row) {
                    $docNo = (string) $row['External Document No_'];
                    $row['Expected Line Count'] = $expectedLineCountByDoc[$docNo] ?? 0;
                }
                unset($row);

                Log::info('Expected line count by document', $expectedLineCountByDoc);

                if (!empty($droppedDuplicateKeys)) {
                    Log::warning("DocWyn duplicate keys dropped for customer {$customer}", [
                        'duplicate_count' => count($droppedDuplicateKeys),
                        'sample_keys' => array_slice($droppedDuplicateKeys, 0, 10),
                    ]);
                }

                Log::info("DocWyn prepared rows for customer {$customer}", [
                    'api_rows' => count($responseData),
                    'prepared_rows' => count($arrays_to_insert240),
                    'invalid_rows' => $invalidRowsCount,
                    'blocked_rows' => $blockedRowsCount,
                    'invalid_uom_rows' => $invalidUomRowsCount,
                    'invalid_quantity_rows' => $invalidQuantityRowsCount,
                    'duplicate_rows' => count($droppedDuplicateKeys),
                ]);

                $previewRows = array_merge($previewRows, $arrays_to_insert240);
                $customerSummaries[] = [
                    'customer' => $customer,
                    'api_rows' => count($responseData),
                    'prepared_rows' => count($arrays_to_insert240),
                    'invalid_rows' => $invalidRowsCount,
                    'blocked_rows' => $blockedRowsCount,
                    'invalid_uom_rows' => $invalidUomRowsCount,
                    'invalid_quantity_rows' => $invalidQuantityRowsCount,
                    'duplicate_rows' => count($droppedDuplicateKeys),
                    'duplicate_samples' => $customerDuplicateSamples,
                ];

            } catch (\Exception $e) {
                Log::error('Exception in ' . __METHOD__ . '(): ' . $e->getMessage());
                return response()->json(['error' => $e->getMessage(), 'action' => 'Docwyn fetch Api', 'timestamp' => now()]);
            }
        }

        return response()->json([
            'action' => 'Docwyn fetch Api preview',
            'timestamp' => now()->addHours(3),
            'company' => $company,
            'received_date' => $receivedDate,
            'customers' => $customers,
            'total_prepared_rows' => count($previewRows),
            'duplicate_samples' => $duplicateSamples,
            'customer_summaries' => $customerSummaries,
            'data' => $previewRows,
        ]);
    }

    private function fetchDocwynCustomerData(string $key, string $company, string $receivedDate, int $customer): array
    {
        $url = config('app.fetch_save_docwyn_api')
            . $key
            . '&company=' . $company
            . '&recieved_date=' . $receivedDate
            . '&cust_no=' . $customer;

        $verifySsl = (bool) config('app.docwyn_verify_ssl', false);
        $caBundlePath = trim((string) config('app.docwyn_ca_bundle', ''));

        $httpClient = Http::timeout(60);

        if ($verifySsl) {
            if ($caBundlePath !== '' && file_exists($caBundlePath)) {
                $httpClient = $httpClient->withOptions(['verify' => $caBundlePath]);
            } else {
                $httpClient = $httpClient->withOptions(['verify' => true]);
            }
        } else {
            $httpClient = $httpClient->withoutVerifying();
        }

        $response = $httpClient->get($url);

        return $response->json() ?? [];
    }

    public function fetchAndSaveShopInvoices()
    {
        $url = config('app.fetch_shop_invoices_api');

        $helpers = new Helpers();
        $response = $helpers->send_curl($url, null);

        if (empty($response)) {
            return response()->json(['success' => true, 'message' => 'No data to insert invoices.', 'timestamp' => now()->addHours(3)]);
        }

        // Decode response and extract external document numbers
        $invoices = json_decode($response, true);
        $extNos = array_column($invoices, 'extdocno');

        // Calculate safe batch size
        $columnsPerRecord = 19; // Number of columns in each record
        $maxBatchSize = floor(2100 / $columnsPerRecord) - 5; // Subtracting for query overhead

        try {
            DB::beginTransaction();

            // Process data in chunks
            $chunks = array_chunk($invoices, $maxBatchSize);

            foreach ($chunks as $chunk) {
                $upsertData = [];

                foreach ($chunk as $invoice) {
                    $upsertData[] = [
                        'ExtDocNo' => strtoupper($invoice['extdocno']),
                        'LineNo' => $invoice['line_no'],
                        'CustNO' => $invoice['cust_no'],
                        'Date' => $invoice['date'],
                        'SPCode' => $invoice['sp_code'],
                        'ItemNo' => $invoice['item_code'],
                        'Qty' => (float)$invoice['qty'],
                        'UnitPrice' => (float)$invoice['price'],
                        'TotalHeaderAmount' => (float)$invoice['total_amt'],
                        'LineAmount' => (float)$invoice['line_amount'],
                        'TotalHeaderQty' => (float)$invoice['total_qty'],
                        'Type' => 2,
                        'Executed' => 0,
                        'Posted' => 0,
                        'ItemBlockedStatus' => 0,
                        'RevertFlag' => 0,
                        'CUInvoiceNo' => $invoice['CuInvoiceNo'],
                        'CUNo' => $invoice['CuNo'],
                        'SigningTime' => $invoice['SignTime'],
                    ];
                }

                // Perform upsert
                if (!empty($upsertData)) {
                    DB::connection('bc240')
                        ->table('FCL1$Imported SalesAL$23dc970e-11e8-4d9b-8613-b7582aec86ba')
                        ->upsert(
                            $upsertData, // Data to insert/update
                            ['ExtDocNo', 'LineNo'], // Unique keys to check for existing records
                            [ // Columns to update if a record exists
                                'CustNO', 'Date', 'SPCode', 'ItemNo', 'Qty', 'UnitPrice', 'TotalHeaderAmount', 
                                'LineAmount', 'TotalHeaderQty', 'Type', 'Executed', 'Posted', 'ItemBlockedStatus', 
                                'RevertFlag', 'CUInvoiceNo', 'CUNo', 'SigningTime'
                            ]
                        );
                }
            }

            // Update the is_imported column in the original table
            $url = config('app.update_imported_invoices');
            $helpers->send_curl($url, json_encode($extNos));

            DB::commit(); // Commit the transaction if everything is successful
            return response()->json(['success' => true, 'action' => 'Shop Invoices synced successfully', 'timestamp' => now()->addHours(3)]);
        } catch (\Exception $e) {
            DB::rollBack(); // Rollback the transaction if an exception occurs

            // Log the error
            Log::error('Shop Invoices Transaction failed: ' . $e->getMessage());
            return response()->json(['Error' => $e->getMessage(), 'action' => 'Shop Invoices sync failed', 'timestamp' => now()->addHours(3)]);
        }
    }

    public function fetchAndSaveFeedMillInvoices()
    {
        $url = config('app.fetch_feedmill_invoices_api');

        $helpers = new Helpers();
        $response = $helpers->send_curl($url, null);

        if (empty($response)) {
            return response()->json(['success' => true, 'message' => 'No data to insert invoices.', 'timestamp' => now()->addHours(3)]);
        }

        // Decode response and extract external document numbers
        $invoices = json_decode($response, true);
        $extNos = array_column($invoices, 'extdocno');

        // Calculate safe batch size
        $columnsPerRecord = 19; // Number of columns in each record
        $maxBatchSize = floor(2100 / $columnsPerRecord) - 5; // Subtracting for query overhead

        try {
            DB::beginTransaction();

            // Process data in chunks
            $chunks = array_chunk($invoices, $maxBatchSize);

            foreach ($chunks as $chunk) {
                $upsertData = [];

                foreach ($chunk as $invoice) {
                    $upsertData[] = [
                        'ExtDocNo' => strtoupper($invoice['extdocno']),
                        'LineNo' => $invoice['line_no'],
                        'CustNO' => 'C00600',
                        'Date' => $invoice['date'],
                        'SPCode' => $invoice['sp_code'],
                        'ItemNo' => $invoice['item_code'],
                        'Qty' => (float)$invoice['qty'],
                        'Location' => 'STR002',
                        'UnitPrice' => (float)$invoice['price'],
                        'TotalHeaderAmount' => (float)$invoice['total_amt'],
                        'LineAmount' => (float)$invoice['line_amount'],
                        'TotalHeaderQty' => (float)$invoice['total_qty'],
                        'Type' => 2,
                        'Executed' => 0,
                        'Posted' => 0,
                        'ItemBlockedStatus' => 0,
                        'RevertFlag' => 0,
                        'CUInvoiceNo' => $invoice['CuInvoiceNo'],
                        'CUNo' => $invoice['CuNo'],
                        'SigningTime' => $invoice['SignTime'],
                    ];
                }

                // Perform upsert
                if (!empty($upsertData)) {
                    DB::connection('bc240')
                        ->table('RMK$Imported SalesAL$23dc970e-11e8-4d9b-8613-b7582aec86ba')
                        ->upsert(
                            $upsertData, // Data to insert/update
                            ['ExtDocNo', 'LineNo'], // Unique keys to check for existing records
                            [ // Columns to update if a record exists
                                'CustNO', 'Date', 'SPCode', 'ItemNo', 'Qty', 'Location', 'UnitPrice', 'TotalHeaderAmount', 
                                'LineAmount', 'TotalHeaderQty', 'Type', 'Executed', 'Posted', 'ItemBlockedStatus', 
                                'RevertFlag', 'CUInvoiceNo', 'CUNo', 'SigningTime'
                            ]
                        );
                }
            }

            // Update the is_imported column in the original table
            $url = config('app.update_imported_invoices');
            $helpers->send_curl($url, json_encode($extNos));

            DB::commit(); // Commit the transaction if everything is successful
            return response()->json(['success' => true, 'action' => 'Feedmill Shop Invoices synced successfully', 'timestamp' => now()->addHours(3)]);
        } catch (\Exception $e) {
            DB::rollBack(); // Rollback the transaction if an exception occurs

            // Log the error
            Log::error('Feedmill Invoices Transaction failed: ' . $e->getMessage());
            return response()->json(['Error' => $e->getMessage(), 'action' => 'Feedmill Invoices sync failed', 'timestamp' => now()->addHours(3)]);
        }
    }

    public function fetchUpdateInvoicesSignatures()
    {
        $blank_invoices = DB::connection('bc240')->table('FCL1$Sales Invoice Header$437dbf0e-84ff-417a-965d-ed2bb9650972 as a')
            // ->join('FCL$Sales Invoice Header as b', function ($join) {
            //     $join->on('a.No_', '=', DB::raw('UPPER(b.No_)'));
            // })
            ->select('a.External Document No_')
            ->whereDate('a.Posting Date', '>=', today()->subDays(2)) //last 2 days invoices
            ->where('a.CUInvoiceNo', '')
            ->where('a.External Document No_', 'like', 'IV-%')
            ->get()
            ->pluck('External Document No_')
            ->toArray();

        $url = config('app.fetch_invoices_signature_api');

        $helpers = new Helpers();

        $response = $helpers->send_curl($url, json_encode($blank_invoices));

        if (empty($response)) {
            return response()->json(['success' => true, 'message' => 'No data to update signatures.', 'timestamp' => now()->addHours(3)]);
        }

        $toUpdateData = json_decode($response, true);

        try {

            DB::beginTransaction();

            foreach ($toUpdateData as $b) {
                $updateQuery = DB::connection('bc240')->table('FCL1$Sales Invoice Header$437dbf0e-84ff-417a-965d-ed2bb9650972 as a')
                    // ->join('FCL$Sales Invoice Header as b', function ($join) {
                    //     $join->on('a.No_', '=', DB::raw('UPPER(b.No_)'));
                    // })
                    ->where('a.External Document No_', $b['External_doc_no'])
                    ->update([
                        'a.SignTime' => $b['SignTime'],
                        'a.CUNo' => $b['CuNo'],
                        'a.CUInvoiceNo' => $b['CuInvoiceNo']
                    ]);
            }

            DB::commit();
            return response()->json(['success' => true, 'action' => 'fetchUpdateInvoicesSignatures()', 'timestamp' => now()->addHours(3)]);
        } catch (\Exception $e) {
            DB::rollBack();
            Log::error('Exception in fetchUpdateInvoicesSignatures(): ' . $e->getMessage());
            return response()->json(['Error' => $e->getMessage(), 'action' => 'fetchUpdateInvoicesSignatures()', 'timestamp' => now()->addHours(3)]);
        }
    }

    public function fetchInsertPortalCustomers()
    {
        $customers = DB::connection('bc240')->table('FCL1$Customer$437dbf0e-84ff-417a-965d-ed2bb9650972 as a')
            ->join('FCL1$Customer$437dbf0e-84ff-417a-965d-ed2bb9650972$ext as b', 'a.No_', '=', 'b.No_')
            ->select('a.No_ as customer_no', 'a.Name as customer_name', 'a.Phone No_ as customer_phone')
            ->where('b.Web Portal$23dc970e-11e8-4d9b-8613-b7582aec86ba', 1)
            ->where('a.Name', '!=', '')
            ->distinct()
            ->get();

        $url = config('app.insert_portal_customers_api');

        $helpers = new Helpers();

        $response = $helpers->send_curl($url, json_encode($customers));

        return response()->json($response);
        // return response(json_encode($customers));
    }

    public function fetchInsertPortalCustomersAddresses()
    {
        $addresses = DB::connection('bc240')
            ->table('FCL1$Ship-to Address$437dbf0e-84ff-417a-965d-ed2bb9650972 as a')
            ->join('FCL1$Customer$437dbf0e-84ff-417a-965d-ed2bb9650972 as c', 'a.Customer No_', '=', 'c.No_')
            ->join('FCL1$Customer$437dbf0e-84ff-417a-965d-ed2bb9650972$ext as b', 'c.No_', '=', 'b.No_')
            ->select('a.Customer No_ as customer_no', DB::raw("'' as route_code"), 'a.Code as ship_code', 'a.Name as ship_to_name')
            ->where('b.Web Portal$23dc970e-11e8-4d9b-8613-b7582aec86ba', 1)
            ->where('a.Name', '!=', '')
            ->distinct()
            ->orderBy('a.Customer No_')
            ->get();

        $url = config('app.insert_portal_shipping_addresses_api');

        $helpers = new Helpers();

        $response = $helpers->send_curl($url, json_encode($addresses));

        return response()->json($response);
        // return response(json_encode($addresses));
    }

    public function fetchMpesaPayments()
    {
        $url = config('app.fetch_mpesa_transactions_api');
        // $data = Http::timeout(60)->post($url);
        return $url;
    }

    public function insertMpesaPayments()
    {
        $data = $this->fetchMpesaPayments();

        return $data;

        try {
            //code...
            foreach ($data as $d) {
                DB::connection('sales')
                ->table('FCL$MPESA Confirmation')
                ->upsert(
                    [
                        [
                            'Confirmation Code' => $d->TransID,
                            'Short Code' => $d->BusinessShortCode,
                            'Name' => $d->FirstName,
                            'Amount' => $d->TransAmount,
                            'Date' => $d->TransTime,
                        ]
                    ], 
                    ['Confirmation Code'], // Columns to check for conflict
                    ['Short Code', 'Name', 'Amount', 'Date'] // Columns to update if conflict occurs
                );
            }

            return response()->json(['success' => true, 'action' => 'action at ' . __METHOD__ . '', 'timestamp' => now()->addHours(3)]);
        } catch (\Exception $e) {
            Log::error('Exception in ' . __METHOD__ . ': ' . $e->getMessage());
            return response()->json(['Error' => $e->getMessage(), 'action' => 'action at' . __METHOD__ . '', 'timestamp' => now()->addHours(3)]);
        }
    }
}
