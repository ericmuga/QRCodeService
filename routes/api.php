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

Route::get('/fetch/orders', [ApiServiceController::class, 'getPortalOrdersApi']);
