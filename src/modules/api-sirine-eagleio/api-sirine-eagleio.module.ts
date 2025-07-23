import { Module } from '@nestjs/common';
import { ApiSirineEagleioService } from './api-sirine-eagleio.service';
import { ApiSirineEagleioController } from './api-sirine-eagleio.controller';

import { HttpModule } from '@nestjs/axios';
import { SirineModule } from '../sirine/sirine.module';

@Module({
  imports: [HttpModule, SirineModule],
  controllers: [ApiSirineEagleioController],
  providers: [ApiSirineEagleioService],
})
export class ApiSirineEagleioModule {}
