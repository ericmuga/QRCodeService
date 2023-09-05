<?php

namespace App\Http\Controllers;

use Illuminate\Http\Request;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log;

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
        $res = ['success' => $action, 'action' => 'Orders Insert', 'timestamp' => now('Africa/Nairobi')];
        return response()->json($res);
    }

    public function insertPortalOrders($data)
    {
        $shops_customer_codes = ['96279', '99850', '95263', '94600', '97096', '93175', '99073'];

        try {
            // try insert
            foreach ($data as $d) {

                if (in_array($d['customer_code'], $shops_customer_codes)) {
                    # insert for shops...
                    $existingRecord = DB::table('FCL$Imported Orders')
                        ->where('External Document No_', $d['tracking_no'])
                        ->where('Line No_', $d['id'])
                        ->first();

                    if (!$existingRecord) {
                        DB::table('FCL$Imported Orders')->insert([
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
                            'Status' => 0
                        ]);
                    }
                } else {
                    # insert into sales
                    $existingRecord = DB::connection('sales')->table('FCL$Imported Orders')
                        ->where('External Document No_', $d['tracking_no'])
                        ->where('Line No_', $d['id'])
                        ->first();

                    if (!$existingRecord) {
                        DB::connection('sales')->table('FCL$Imported Orders')->insert([
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
                            'Customer Specification-H' => $d['customer_specification'],
                            'Customer Specification-L' => $d['product_specifications'],
                        ]);
                    }
                }
            }

            return true;
        } catch (\Exception $e) {
            Log::error('Exception in ' . __METHOD__ . '(): ' . $e->getMessage());
            return false;
        }
    }

    public function getStatus()
    {
        $mainRecords = DB::table('FCL$Imported Orders')
            ->whereDate('Shipment Date', '>=', today())
            ->get();

        $salesRecords = DB::connection('sales')->table('FCL$Imported Orders')
            ->whereDate('Shipment Date', '>=', today())
            ->get();

        dd($mainRecords);
    }

    public function updateStatus()
    {
    }

    public function getVendorList()
    {
        $results = DB::table('FCL$Vendor as a')
            ->join('FCL$SlaughterData as b', 'a.No_', '=', 'b.VendorNo')
            ->select(
                'a.No_',
                'a.Phone No_',
                'a.Contact',
                'a.Name',
                'b.Settlement Date AS settlement_date',
            )
            ->where('a.Vendor Posting Group', 'PIGFARMERS')
            ->where('b.Settlement Date', '>=', '2022-01-01 00:00:00.000')
            ->orderBy('b.Settlement Date', 'asc')
            ->groupBy('a.No_', 'a.Phone No_', 'a.Contact', 'a.Name', 'b.Settlement Date')
            ->get();

        $action = false;

        if (!empty($results)) {
            $action = $this->insertVendorListApi($results);
        }

        // Return the response
        $res = ['success' => $action, 'action' => 'Vendors List Insert', 'timestamp' => now('Africa/Nairobi')];

        return response()->json($res);
    }

    public function insertVendorListApi($data)
    {
        try {
            // try insert
            foreach ($data as $d) {
                # insert into sales
                // $existingRecord = DB::connection('orders')->table('FCL$Imported Orders')
                //     ->where('External Document No_', $d['tracking_no'])
                //     ->where('Line No_', $d['id'])
                //     ->first();

                // if (!$existingRecord) {
                //     DB::connection('orders')->table('FCL$Imported Orders')->insert([
                //         'External Document No_' => $d['tracking_no'],
                //         'Line No_' => $d['id'],
                //         'Sell-to Customer No_' => $d['customer_code'],
                //         'Shipment Date' => $d['shipment_date'],
                //         'Salesperson Code' => $d['sales_code'],
                //         'Ship-to Code' => $d['ship_to_code'],
                //         'Ship-to Name' => $d['ship_to_name'],
                //         'Item No_' => $d['item_code'],
                //         'Quantity' => $d['quantity'],
                //         'Unit of Measure' => $d['unit_of_measure'],
                //         'Status' => 0,
                //         'Customer Specification-H' => $d['customer_specification'],
                //         'Customer Specification-L' => $d['product_specifications'],
                //     ]);
                // }
            }

            return true;
        } catch (\Exception $e) {
            Log::error('Exception in ' . __METHOD__ . '(): ' . $e->getMessage());
            return false;
        }
    }

    public function sendCurl($url)
    {
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

        // response
        return $response;
    }
}
