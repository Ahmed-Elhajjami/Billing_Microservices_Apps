import {Component, OnInit} from '@angular/core';
import {HttpClient} from '@angular/common/http';
import {ActivatedRoute, Router} from '@angular/router';

@Component({
  selector: 'app-bills-details',
  standalone: false,
  templateUrl: './bills-details.component.html',
  styleUrl: './bills-details.component.css'
})
export class BillsDetailsComponent implements OnInit {
  billsDetails: any;
  billsId!: number;

  constructor(private http: HttpClient, private router: Router, private route: ActivatedRoute) {
    this.billsId = route.snapshot.params['billsId'];
  }

  ngOnInit(): void {
    // ✅ CORRIGÉ : Changé bills-service en billing-service et ajouté /bills/
    this.http.get("http://localhost:8888/billing-service/full/" + this.billsId)
      .subscribe({
        next: (data) => {
          this.billsDetails = data;
          console.log('✅ Bill details:', this.billsDetails);
        },
        error: (err) => {
          console.error('Error:', err);
        }
      });
  }

  // ✅ AJOUTÉ : Méthode pour calculer le total
  getTotal(): number {
    if (!this.billsDetails || !this.billsDetails.productItems) {
      return 0;
    }
    return this.billsDetails.productItems.reduce(
      (total: number, item: any) => total + (item.quantity * item.unitPrice),
      0
    );
  }

  // ✅ AJOUTÉ : Méthode pour retourner à la liste
  goBack(): void {
    this.router.navigate(['/bills', this.billsDetails.customerId]);
  }

  getBillsDetails(b: any) {
    this.router.navigateByUrl("/bills-details/" + b.id);
  }
}
