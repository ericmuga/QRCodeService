<?php

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

Route::post('/generate/',function(Request $request)
                            {

                                QrCode::format('png')
                                    ->generate($request->text,public_path($request->filename.'png'));

                                return response('Success', 200)
                                        ->header('Content-Type', 'text/plain');
                            }
                        );

