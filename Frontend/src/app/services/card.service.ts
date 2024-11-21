import {Injectable, signal} from '@angular/core';
import {Card} from '../interfaces/card';

@Injectable({
  providedIn: 'root'
})
export class CardService {
  searchedCards = signal<Card[]>([]);

  constructor() { }
}
