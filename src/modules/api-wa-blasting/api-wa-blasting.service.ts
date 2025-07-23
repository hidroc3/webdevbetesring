import { HttpService } from '@nestjs/axios';
import { Injectable } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { AxiosError } from 'axios';
import { catchError, firstValueFrom } from 'rxjs';
import { SendFileDto } from './dto/send-file.dto';
import { SendImageDto } from './dto/send-image.dto';
import { SendMessageDto } from './dto/send-message.dto';

@Injectable()
export class ApiWaBlastingService {
  private readonly WAZAP_API_URL: string;
  private readonly WAZAP_INSTANCE_ID: string;
  private readonly WAZAP_API_TOKEN: string;

  constructor(
    private readonly httpService: HttpService,
    private readonly configService: ConfigService,
  ) {
    this.WAZAP_API_URL = this.configService.get<string>('WAZAP_API_URL')!;
    this.WAZAP_INSTANCE_ID =
      this.configService.get<string>('WAZAP_INSTANCE_ID')!;
    this.WAZAP_API_TOKEN = this.configService.get<string>('WAZAP_API_TOKEN')!;
  }

  async sendMessage(sendMessageDto: SendMessageDto) {
    const { phoneNumber, message } = sendMessageDto;
    const data = {
      instance_id: this.WAZAP_INSTANCE_ID,
      apimeta: {
        api_token: this.WAZAP_API_TOKEN,
      },
      wa_messages: [
        {
          to: phoneNumber,
          body: message,
        },
      ],
    };

    return this.sendRequest(data, 'send_text_message');
  }

  async sendImage(sendImageDto: SendImageDto) {
    const { phoneNumber, imageUrl, caption } = sendImageDto;
    const data = {
      instance_id: this.WAZAP_INSTANCE_ID,
      apimeta: {
        api_token: this.WAZAP_API_TOKEN,
      },
      wa_messages: [
        {
          to: phoneNumber,
          media_url: imageUrl,
          body: caption,
        },
      ],
    };
    return this.sendRequest(data, 'send_image');
  }

  async sendFile(sendFileDto: SendFileDto) {
    const { phoneNumber, fileUrl, caption, filename } = sendFileDto;
    const data = {
      instance_id: this.WAZAP_INSTANCE_ID,
      apimeta: {
        api_token: this.WAZAP_API_TOKEN,
      },
      wa_messages: [
        {
          to: phoneNumber,
          media_url: fileUrl,
          file_name: filename,
          body: caption,
        },
      ],
    };
    return this.sendRequest(data, 'send_file');
  }

  private async sendRequest(data: any, endpoint: string) {
    const url = `${this.WAZAP_API_URL}/${endpoint}`;
    try {
      const { data: response } = await firstValueFrom(
        this.httpService.post(url, data).pipe(
          catchError((error: AxiosError) => {
            console.error(
              'Error during API request:',
              error.response?.data || error.message,
            );
            throw `API Request Failed: ${error.message}`;
          }),
        ),
      );
      return response;
    } catch (error) {
      console.error('Failed to send request:', error);
      throw error;
    }
  }
}
