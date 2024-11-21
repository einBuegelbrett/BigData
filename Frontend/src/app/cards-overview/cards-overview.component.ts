import {Component, effect} from '@angular/core';
import {CardService} from '../services/card.service';
import {Card} from '../interfaces/card';

@Component({
  selector: 'app-cards-overview',
  standalone: true,
  imports: [],
  templateUrl: './cards-overview.component.html',
  styleUrl: './cards-overview.component.scss'
})
export class CardsOverviewComponent {
  cards: Card[] = [];

  constructor(private cardService: CardService) {
    effect(() => {
      this.cards = this.cardService.searchedCards();
    });
  }
}
