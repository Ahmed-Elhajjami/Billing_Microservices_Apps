import {Component, OnInit} from '@angular/core';
import {HttpClient} from '@angular/common/http';
import {ActivatedRoute, Router} from '@angular/router';

@Component({
  selector: 'app-bills',
  standalone: false,
  templateUrl: './bills.component.html',
  styleUrl: './bills.component.css'
})
export class BillsComponent implements OnInit {
  bills: any;
  customerId!: number;

  constructor(private http: HttpClient, private router: Router, private route: ActivatedRoute) {
    this.customerId = route.snapshot.params['customerId'];
  }

  ngOnInit(): void {
    console.log('🔍 [BILLS] Component initialisé');
    this.http.get("http://localhost:8888/billing-service/api/bills/search/byCustomerId?customerId=" + this.customerId)
      .subscribe({
        next: (data) => {
          this.bills = data;
          console.log('✅ Bills loaded:', this.bills);
        },
        error: (err) => {
          console.error('Error:', err);
        }
      });
  }

  getBillsDetails(b: any) {
    this.router.navigateByUrl("/bills-details/" + b.id);
  }

  // ✅ AJOUTÉ : Méthode pour retourner à la liste des customers
  goBack(): void {
    this.router.navigate(['/customers']);
  }
}
