import { FormBuilder, FormGroup } from '@angular/forms';
import { HttpClient } from '@angular/common/http';

export class MyComponent {
  myForm: FormGroup;
  selectedFiles: File[] = [];

  constructor(private fb: FormBuilder, private http: HttpClient) {
    this.myForm = this.fb.group({
      name: ['']
      // … other fields
    });
  }

  onFilesSelected(event: any) {
    this.selectedFiles = Array.from(event.target.files);
  }

  onSubmit() {
    const formData = new FormData();
    formData.append('name', this.myForm.get('name')?.value);

    // Append multiple files
    this.selectedFiles.forEach((file, index) => {
      formData.append('source_files', file, file.name);
      // or you could do: formData.append('source_files[]', file, file.name);
      // depending on how the backend expects it
    });

    this.http.post('/your-api-endpoint', formData).subscribe(
      res => console.log('Success', res),
      err => console.error('Error', err)
    );
  }
}
