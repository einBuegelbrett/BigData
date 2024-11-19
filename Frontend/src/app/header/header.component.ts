import { Component } from '@angular/core';
import {FormsModule} from '@angular/forms';
import {ApiService} from '../services/api.service';

@Component({
  selector: 'app-header',
  standalone: true,
  imports: [
    FormsModule
  ],
  templateUrl: './header.component.html',
  styleUrl: './header.component.scss'
})
export class HeaderComponent {
  searchText = '';

  constructor(private apiService: ApiService) { }

  searchCard() {
    this.apiService.getCards().subscribe({
      next: (res) => {
        console.log(res);
      },
      error: (error) => {
        console.error('Error occurred:', error);
      }
    });
  }
}
