import {Component, OnInit} from '@angular/core';
import {FormsModule} from '@angular/forms';
import {ApiService} from '../services/api.service';
import {CardService} from '../services/card.service';
import {Card} from '../interfaces/card';

@Component({
  selector: 'app-header',
  standalone: true,
  imports: [
    FormsModule
  ],
  templateUrl: './header.component.html',
  styleUrl: './header.component.scss'
})
export class HeaderComponent implements OnInit {
  searchText = '';

  constructor(private apiService: ApiService, private cardService: CardService) {
  }

  ngOnInit() {
    this.apiService.getCards().subscribe({
      next: (res: Card[]) => {
        this.cardService.searchedCards.set(res);
        console.log(res)
      },
      error: (error) => {
        console.error('Error occurred:', error);
      }
    });
  }

  searchCard() {
    if(this.searchText === '') {
      this.apiService.getCards().subscribe({
        next: (res) => {
          console.log(res);
          this.cardService.searchedCards.set(res);
        },
        error: (error) => {
          console.error('Error occurred:', error);
        }
      });
    } else {
      this.apiService.getCardByName(this.searchText).subscribe({
        next: (res) => {
          console.log(res);
          this.cardService.searchedCards.set(res);
        },
        error: (error) => {
          console.error('Error occurred:', error);
        }
      });
    }
  }
}
