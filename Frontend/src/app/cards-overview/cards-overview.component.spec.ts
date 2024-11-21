import { ComponentFixture, TestBed } from '@angular/core/testing';

import { CardsOverviewComponent } from './cards-overview.component';

describe('CardsOverviewComponent', () => {
  let component: CardsOverviewComponent;
  let fixture: ComponentFixture<CardsOverviewComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [CardsOverviewComponent]
    })
    .compileComponents();

    fixture = TestBed.createComponent(CardsOverviewComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
