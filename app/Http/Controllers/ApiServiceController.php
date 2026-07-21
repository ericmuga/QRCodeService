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

    // public function fetchDocwynDataAndSave(Request $request)
    // {
    //     $company = $request->has('company') ? $request->company : 'FCL';
    //     $receivedDate = Carbon::today()->toDateString();
    //     $key = config('app.docwyn_api_key');
    //     $from = 0;
    //     $to = 150;

    //     $customers = [404, 240, 258, 913, 914, 420, 823, 824];
    //     // $customers = [913];

    //     foreach ($customers as $customer) {
    //         $fromRange = $from;
    //         $toRange = $to;

    //         do {
    //             $response = Http::get(config('app.fetch_save_docwyn_api') . $key . '&company=' . $company . '&recieved_date=' . $receivedDate . '&cust_no=' . $customer . '&from=' . $fromRange . '&to=' . $toRange);

    //             $responseData = $response->json();

    //             if (empty($responseData)) {
    //                 break;
    //             }

    //             $extdocItem = '';
    //             // $arrays_to_insert = [];
    //             $arrays_to_insert240 = [];

    //             $collection = collect($responseData);

    //             Log::info('DocWyn Data fetched for insert: ');
    //             Log::info($collection);

    //             $sortedData = $collection->sortBy('ext_doc_no')->sortBy('item_no')->values();

    //             foreach ($sortedData as $data) {
    //                 if (!is_array($data) || !array_key_exists('ext_doc_no', $data)) {
    //                     continue;
    //                 }

    //                 if ($extdocItem == $data['ext_doc_no'] . $data['item_no']) {
    //                     continue;
    //                 }

    //                 // Replace escaped slashes with normal slashes in shp_date
    //                 $cleanedDate = str_replace('\/', '/', $data['shp_date']);

    //                 // Convert the cleaned shp_date to the desired format
    //                 // $shipmentDate = Carbon::createFromFormat('d/m/Y', $cleanedDate)
    //                 // ->startOfDay() // Sets the time to 00:00:00.000
    //                 // ->format('Y-m-d H:i:s.000');

    //                 $shipmentDate = Carbon::today()->format('Y-m-d H:i:s.000');

    //                 // Log or process the converted date
    //                 Log::info("Converted Shipment Date: $shipmentDate");

    //                 $arrays_to_insert240[] = [
    //                     'Company' => $data['company'],
    //                     'Sell-to Customer No_' => $data['cust_no'],
    //                     'Customer Specification' => $data['cust_spec'],
    //                     'External Document No_' => $data['ext_doc_no'],
    //                     'Item No_' => $data['item_no'],
    //                     'Line No_' => $data['line_no'],
    //                     'Quantity' => abs(intval($data['quantity'])),
    //                     'Ship-to Code' => $data['shp_code'],
    //                     'Shipment Date' => $shipmentDate,
    //                     'Salesperson Code' => $data['sp_code'],
    //                     'Unit of Measure' => '',
    //                 ];

    //                 $extdocItem = $data['ext_doc_no'] . $data['item_no'];
    //             }

    //             try {
    //                 if (!empty($arrays_to_insert240)) {
    //                     DB::connection('bc240')->table('FCL1$Imported Orders$23dc970e-11e8-4d9b-8613-b7582aec86ba')->upsert(
    //                         $arrays_to_insert240, 
    //                         ['External Document No_', 'Item No_'], // Unique keys
    //                         ['Quantity', 'Shipment Date'] // Columns to update on conflict
    //                     );
    //                 }
    //             } catch (\Exception $e) {
    //                 Log::error('Exception in ' . __METHOD__ . '(): ' . $e->getMessage());
    //                 return response()->json(['error' => $e->getMessage(), 'action' => 'Docwyn fetch & Insert', 'timestamp' => now()->addHours(3)]);
    //             }

    //             $fromRange = $toRange + 1;
    //             $toRange = $fromRange + 100;

    //         } while (count($responseData) > 0);
    //     }

    //     return response()->json(['message' => 'Data saved successfully', 'action' => 'Docwyn fetch & Insert', 'timestamp' => now()->addHours(3)]);
    // }

    public function fetchDocwynDataAndSave(Request $request)
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

        foreach ($customers as $customer) {
            try {
                // Fetch all data for the customer
                $response = Http::get(config('app.fetch_save_docwyn_api') . $key . '&company=' . $company . '&recieved_date=' . $receivedDate . '&cust_no=' . $customer);
                $responseData = $response->json();

                if (empty($responseData)) {
                    continue;
                }

                $processedItems = []; // Keep track of processed items
                $arrays_to_insert240 = [];
                $droppedDuplicateKeys = [];
                $invalidRowsCount = 0;
                $blockedRowsCount = 0;
                $invalidUomRowsCount = 0;
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

                    $lineNo = (string) $data['line_no'];

                    // Match dedupe key with DB unique index (External Document No_ + Line No_)
                    $uniqueKey = $externalDocNo . '-' . $lineNo;
                    
                    if (isset($processedItems[$uniqueKey])) {
                        $droppedDuplicateKeys[] = $uniqueKey;
                        continue;
                    }

                    $shipmentDate = Carbon::today()->format('Y-m-d H:i:s.000');

                    $arrays_to_insert240[] = [
                        'Company' => $data['company'],
                        'Sell-to Customer No_' => $data['cust_no'],
                        'Customer Specification' => $data['cust_spec'],
                        'Product Specification' => $data['item_spec'],
                        'Expected Line Count' => 0,
                        'Error Message' => '',
                        'PDA Order' => 0,
                        'External Document No_' => $externalDocNo,
                        'Item No_' => $data['item_no'],
                        'Line No_' => $lineNo,
                        'Quantity' => abs(intval($data['quantity'])),
                        'Ship-to Code' => $data['shp_code'],
                        'Shipment Date' => $shipmentDate,
                        'Salesperson Code' => $data['sp_code'],
                        'Unit of Measure' => $uomCode,
                    ];

                    $processedItems[$uniqueKey] = true;
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
    
    // public function fetchAndSaveShopInvoicesCustom()
    // {
    //     $url = config('app.fetch_shop_invoices_api_custom');

    //     $helpers = new Helpers();
    //     $response = $helpers->send_curl($url, $post_data = null);

    //     if (empty($response)) {
    //         return response()->json(['success' => true, 'message' => 'No data to insert invoices.', 'timestamp' => now()->addHours(3)]);
    //     }

    //     // Insert the results into the new database
    //     $invoices = json_decode($response, true);
    //     $extNos = array_column($invoices, 'extdocno');

    //     try {
    //         DB::beginTransaction();

    //         foreach ($invoices as $invoice) {
    //             // Insert the data into the new table
    //             $extDocNo = strtoupper($invoice['extdocno']);
    //             $lineNo = $invoice['line_no'];

    //             // Check if the combination exists in the table
    //             $existingRecord = DB::connection('bc240')->table('FCL1$Imported Sales$23dc970e-11e8-4d9b-8613-b7582aec86ba')
    //                 ->where('ExtDocNo', $extDocNo)
    //                 ->where('LineNo', $lineNo)
    //                 ->first();

    //             if (!$existingRecord) {
    //                 DB::connection('bc240')->table('FCL1$Imported Sales$23dc970e-11e8-4d9b-8613-b7582aec86ba')->insert([
    //                     'ExtDocNo' => $extDocNo,
    //                     'LineNo' => $lineNo,
    //                     'ItemNo' => $invoice['item_code'],
    //                     'CustNO' => $invoice['cust_no'],
    //                     'Date' => $invoice['date'],
    //                     'ShiptoCOde' => '',
    //                     'Location' => '',
    //                     'ShiptoName' => '',
    //                     'SUOM' => '',
    //                     'SPCode' => $invoice['shop_code'],
    //                     'Qty' => $invoice['qty'],
    //                     'UnitPrice' => $invoice['price'],
    //                     'LineAmount' => $invoice['line_amount'],
    //                     'TotalHeaderAmount' => $invoice['total_amt'],
    //                     'TotalHeaderQty' => $invoice['total_qty'],
    //                     'Type' => 2,
    //                     'Executed' => 0,
    //                     'Posted' => 0,
    //                     'ItemBlockedStatus' => 0,
    //                     'RevertFlag' => 0,
    //                 ]);
    //             }
    //         }

    //         // Update the is_imported column in the original table
    //         $url = config('app.update_imported_invoices');
    //         $response = $helpers->send_curl($url, json_encode($extNos));

    //         DB::commit(); // Commit the transaction if everything is successful
    //         return response()->json(['success' => true, 'action' => 'Shop Invoices synced successfully', 'timestamp' => now()->addHours(3)]);
    //     } catch (\Exception $e) {
    //         DB::rollBack(); // Rollback the transaction if an exception occurs

    //         // Handle the exception (log, throw, or other custom logic)
    //         Log::error('Shop Invoices Transaction failed: ' . $e->getMessage());
    //         return response()->json(['Error' => $e->getMessage(), 'action' => 'Shop Invoices sync failed', 'timestamp' => now()->addHours(3)]);
    //     }
    // }

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

    // public function fetchUpdateSpecificInvoicesSignatures()
    // {
    //     $url = config('app.fetch_specific_invoices_signature_api');

    //     $helpers = new Helpers();

    //     $response = $helpers->send_curl($url);

    //     if (empty($response)) {
    //         return response()->json(['success' => true, 'message' => 'No data to update signatures.', 'timestamp' => now()->addHours(3)]);
    //     }

    //     $toUpdateData = json_decode($response, true);

    //     try {

    //         DB::beginTransaction();

    //         foreach ($toUpdateData as $b) {
    //             $updateQuery = DB::connection('bc240')->table('FCL1$Sales Invoice Header$437dbf0e-84ff-417a-965d-ed2bb9650972 as a')
    //                 // ->join('FCL$Sales Invoice Header as b', function ($join) {
    //                 //     $join->on('a.No_', '=', DB::raw('UPPER(b.No_)'));
    //                 // })
    //                 ->where('a.External Document No_', $b['External_doc_no'])
    //                 ->update([
    //                     'a.SignTime' => $b['SignTime'],
    //                     'a.CUNo' => $b['CuNo'],
    //                     'a.CUInvoiceNo' => $b['CuInvoiceNo']
    //                 ]);
    //         }

    //         DB::commit();
    //         return response()->json(['success' => true, 'action' => 'action at ' . __METHOD__ . '', 'timestamp' => now()->addHours(3)]);
    //     } catch (\Exception $e) {
    //         DB::rollBack();
    //         Log::error('Exception in ' . __METHOD__ . ': ' . $e->getMessage());
    //         return response()->json(['Error' => $e->getMessage(), 'action' => 'action at' . __METHOD__ . '', 'timestamp' => now()->addHours(3)]);
    //     }
    // }

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
