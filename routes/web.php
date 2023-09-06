<?php

use App\Http\Controllers\ApiServiceController;
use Illuminate\Support\Facades\Route;
use SimpleSoftwareIO\QrCode\Facades\QrCode;
use Illuminate\Http\Request;

/*
|--------------------------------------------------------------------------
| Web Routes
|--------------------------------------------------------------------------
|
| Here is where you can register web routes for your application. These
| routes are loaded by the RouteServiceProvider within a group which
| contains the "web" middleware group. Now create something great!
|
*/

// routes/web.php

Route::get('/logs', '\Rap2hpoutre\LaravelLogViewer\LogViewerController@index')->middleware('web');

Route::get('/', function () {
    return view('welcome');
});

Route::post('/generate', fn (Request $request) => QrCode::format('png')->generate($request->text, public_path($request->filename . 'png')));

//Portal orders
Route::get('/portal/orders', function () {
    return view('welcome');
});

Route::get('/getOrders', [ApiServiceController::class, 'getPortalOrdersApi']);
Route::get('/vendor/list', [ApiServiceController::class, 'getVendorList']);
Route::get('/orders/status/main', [ApiServiceController::class, 'ordersStatusMain']);
