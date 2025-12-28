import { NgModule } from '@angular/core';
import { BrowserModule } from '@angular/platform-browser';
import { HttpClientModule } from '@angular/common/http';
import { CommonModule } from '@angular/common';

import { AppRoutingModule } from './app-routing.module';
import { AppComponent } from './app.component';
import { ProductsComponent } from './products/products.component';
import { CustomersComponent } from './customers/customers.component';
import { BillsComponent } from './bills/bills.component';
import { BillsDetailsComponent } from './bills-details/bills-details.component';

@NgModule({
  declarations: [
    AppComponent,
    ProductsComponent,
    CustomersComponent,
    BillsComponent,
    BillsDetailsComponent
  ],
  imports: [
    BrowserModule,
    HttpClientModule,    // Pour HttpClient
    CommonModule,        // Pour les pipes (date, number, etc.)
    AppRoutingModule
  ],
  providers: [],
  bootstrap: [AppComponent]
})
export class AppModule { }
