import { ComponentFixture, TestBed } from '@angular/core/testing';

import { BillsDetailsComponent } from './bills-details.component';

describe('BillsDetailsComponent', () => {
  let component: BillsDetailsComponent;
  let fixture: ComponentFixture<BillsDetailsComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [BillsDetailsComponent]
    })
    .compileComponents();

    fixture = TestBed.createComponent(BillsDetailsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
