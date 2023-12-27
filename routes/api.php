<?php

use App\Http\Controllers\ApiServiceController;
use Illuminate\Http\Request;
use Illuminate\Support\Facades\Route;

/*
|--------------------------------------------------------------------------
| API Routes
|--------------------------------------------------------------------------
|
| Here is where you can register API routes for your application. These
| routes are loaded by the RouteServiceProvider within a group which
| is assigned the "api" middleware group. Enjoy building your API!
|
*/

Route::middleware('auth:sanctum')->get('/user', function (Request $request) {
    return $request->user();
});

Route::get(
    '/generate/{text}',
    function ($text) {

        $path = 'https://itax.kra.go.ke/KRA-Portal/invoiceChk.htm?actionCode=loadPage&invoiceNo=';
        QrCode::format('png')
            ->generate($path . rtrim($text, "."), public_path($text . 'png'));

        return response('Success', 200)
            ->header('Content-Type', 'text/plain');
    }
);

//Orders
Route::get('/fetch/orders', [ApiServiceController::class, 'getPortalOrdersApi']);
Route::get('/update/orders/main', [ApiServiceController::class, 'ordersStatusMain']);
Route::get('/update/orders/sales', [ApiServiceController::class, 'ordersStatusSales']);
Route::get('/fetch-save-docwyn', [ApiServiceController::class, 'fetchDocwynDataAndSave']);

//pig farmers
Route::get('/fetch/vendorList/{from?}/{to?}', [ApiServiceController::class, 'getVendorList']);

//Employees
Route::get('/employee-list', [ApiServiceController::class, 'getEmployeeList']);

//shops invoices
Route::get('/fetch-save-invoices', [ApiServiceController::class, 'fetchAndSaveShopInvoices']);
Route::get('/fetch-save-invoices/custom', [ApiServiceController::class, 'fetchAndSaveShopInvoicesCustom']);
Route::get('/fetch-update-invoices-signatures', [ApiServiceController::class, 'fetchUpdateInvoicesSignatures']);
Route::get('/fetch-update-specific-invoices-signatures', [ApiServiceController::class, 'fetchUpdateSpecificInvoicesSignatures']);

//shipments
Route::get('/fetch/shipments', [ApiServiceController::class, 'fetchSaveShipments']);
Route::get('/fetch/shipment/lines', [ApiServiceController::class, 'fetchSaveShipmentLines']);

//Portal Customers
Route::get('/fetch/insert/portal-customers', [ApiServiceController::class, 'fetchInsertPortalCustomers']);
Route::get('/fetch/insert/portal-customers-addresses', [ApiServiceController::class, 'fetchInsertPortalCustomersAddresses']);
