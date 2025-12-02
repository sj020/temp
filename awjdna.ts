import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';

@Injectable({ providedIn: 'root' })
export class ConfigService {
  constructor(private http: HttpClient) { }

  loadConfig() {
    // relative URL — no host/port
    return this.http.get('/api/config');
  }
}
