import { Test, TestingModule } from '@nestjs/testing';
import { MessageWaService } from './message-wa.service';

describe('MessageWaService', () => {
  let service: MessageWaService;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [MessageWaService],
    }).compile();

    service = module.get<MessageWaService>(MessageWaService);
  });

  it('should be defined', () => {
    expect(service).toBeDefined();
  });
});
