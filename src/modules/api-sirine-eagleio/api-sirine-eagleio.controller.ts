import { Controller, Post } from '@nestjs/common';
import { ApiSirineEagleioService } from './api-sirine-eagleio.service';

@Controller('api-sirine-eagleio')
export class ApiSirineEagleioController {
  constructor(private readonly service: ApiSirineEagleioService) {}

  @Post('sync')
  async syncSirineData() {
    return this.service.fetchAndStoreSirineData();
  }
}
