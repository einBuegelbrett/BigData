import { Injectable } from '@angular/core';
import {HttpClient} from '@angular/common/http';
import {Card} from '../interfaces/card';

@Injectable({
  providedIn: 'root'
})
export class ApiService {

  constructor(private http: HttpClient) { }

  getCards() {
      return this.http.get<Card[]>('http://localhost:3000/cards');
  }

  getCardByName(name: string) {
      return this.http.get<Card[]>(`http://localhost:3000/cards/${name}`);
  }
}
