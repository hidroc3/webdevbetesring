import { Body, Controller, Post } from '@nestjs/common';
import { ApiWaBlastingService } from './api-wa-blasting.service';
import { SendFileDto } from './dto/send-file.dto';
import { SendImageDto } from './dto/send-image.dto';
import { SendMessageDto } from './dto/send-message.dto';
@Controller('api-wa-blasting')
export class ApiWaBlastingController {
  constructor(private readonly apiWaBlastingService: ApiWaBlastingService) {}

  @Post('send-message')
  async sendMessage(@Body() sendMessageDto: SendMessageDto) {
    return this.apiWaBlastingService.sendMessage(sendMessageDto);
  }

  @Post('send-image')
  async sendImage(@Body() sendImageDto: SendImageDto) {
    return this.apiWaBlastingService.sendImage(sendImageDto);
  }

  @Post('send-file')
  async sendFile(@Body() sendFileDto: SendFileDto) {
    return this.apiWaBlastingService.sendFile(sendFileDto);
  }
}
